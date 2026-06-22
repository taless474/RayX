#!/usr/bin/env python3
"""exp38 -- Ray-hosted RayX non-blocking op-lane: over-load RESIDUAL ATTRIBUTION.

DIAGNOSTIC-ONLY (observation-only, synthetic, machine-specific). exp37 showed the
experimental non-blocking op-lane RECOVERS fine-grain compute retention at fixed
num_lanes == hpx_threads under a parked+compute mix at NEAR load (FULL), but only
PARTIALLY at OVER load. exp38 does NOT re-prove head-of-line removal; it attributes
the exp37 OVER-load residual. See the design note:
  experiments/38_ray_hosting_rayx_nonblocking_residual/
      ray_hosting_rayx_nonblocking_residual.md

LEAD OBSERVATION (why H-cap is the first suspect): at exp37 nb-mi4 under OVER load,
qd_mean_T stayed NONZERO (11.1 at HT=4, 15.0 at HT=8) while NEAR load drained to 0.0.
A non-empty lane queue with the consumer pinned at the in-flight cap means the cap was
BINDING -- so the leading candidate is an undersized admission cap below the over-load
offered concurrency (O = 4*HT), not (yet) intrinsic retirement-path cost.

HYPOTHESES (attribution, not feature SUPPORT/STOP). All retention is NORMALIZED to the
C-only baseline at the SAME hpx_threads, so "HT workers are finite" is already priced
into the baseline and finite HT ALONE is never the explanation:
  * H-cap     -- undersized admission cap; raising max_inflight drains qd->~0 AND lifts
                 retention to SUPPORT.
  * H-retire  -- retirement-path cost (.then continuation, per-lane mutex SERIALIZATION
                 + lock-acquire wait, completion->consumer handoff, in-flight
                 accounting); qd->~0 but retention plateaus, and the residual SHRINKS
                 when compute is coarsened (per-op retire rate drops ~10x).
  * H-sat     -- MIXED-workload scheduler/worker pressure BEYOND the C-only baseline
                 (extra suspended park_ms tasks/timers, in-flight continuations,
                 retirements, HPX scheduler-queue depth); qd->~0 but retention plateaus
                 and the residual does NOT shrink with coarser compute. Backlog migrates
                 from the visible lane queue (qd) into the invisible HPX scheduler queue.
  * H-driver  -- CPython submit/retire cadence (run_as_hpx_thread hop per submit);
                 residual persists independent of cap and compute-retire rate.

NOTE on notify_all: it is NOT treated as a likely thundering-herd cost (there is
usually ONE consumer waiter, plus possibly one shutdown waiter). The plausible
retirement cost is per-lane mutex serialization / lock-acquire wait / handoff /
in-flight accounting. exp38 does NOT optimize notify_all / mutex / inflight bookkeeping
and adds NO C++ and NO counters; it is COUNTER-FREE (Probe A + Probe B only).

THE COUNTER-FREE PARTITION (existing max_inflight_per_lane knob only):
  qd_mean_T -> ~0 and retention -> SUPPORT  ........ H-cap
  qd_mean_T -> ~0 but retention plateaus  .......... H-retire / H-sat / H-driver
                                                     (Probe B then splits H-retire,
                                                      which is granularity-sensitive,
                                                      from H-sat/H-driver, which are not)
  qd_mean_T stays > 0 and retention stalls  ........ still cap-bound

PROBES:
  * Probe A (primary): hpx_threads == num_lanes in {4,8}; levels near (FULL control) +
    over (target); fine n_c; serial anchor + non-blocking max_inflight in {4,8,16}; an
    OPTIONAL guarded mi=32 cell (HT=4 and HT=8) is run LAST -- cooperatively suspended
    park_ms tasks are not OS threads, so extending the cap is HPX-safe; churn/knee at
    mi=32 is reported. If mi=32 is unstable, HT=4 (whose sweep reaches mi=32 = 2*O) is
    the decisive H-cap cell and HT=8 is corroborating.
  * Probe B (secondary): HT=8 over only; best max_inflight from Probe A; compare fine
    (n_c=2,000,000) vs coarse (n_c=20,000,000) to drop the compute-retire rate ~10x at
    fixed parked load. Residual SHRINKS with coarser compute => H-retire; PERSISTS =>
    H-sat/H-driver. K_Wp is RECALIBRATED per granularity and the positive
    serial-erosion control is checked PER granularity (coarse may not erode -- exp35).

A pre-run COMPUTE-DEMAND / SATURATION SANITY CALC is printed as a READING AID (never a
gate): at over O = 4*HT > HT, so C-only compute is worker-bound BY CONSTRUCTION (only HT
run at once) -- retention normalizes against that, so an H-sat residual is MIXED pressure
beyond it; raising max_inflight drains qd but migrates excess compute into the invisible
HPX scheduler queue (the H-sat signature).

GATES (the ONLY pass/fail): agg_ok, futures_completed, plain_types_ok, lane_ids_ok,
clean_shutdown. Timing / retention / qd / lane_stats / attribution are READING criteria
only. p50/p90/p99 retention is reported but the TAIL is DIRECTIONAL (reps=3). Exit 0 =
gates passed or cleanly skipped; exit 1 = a structural gate failed.

Non-blocking is EXPERIMENTAL and OPERATION-LANE-ONLY (actor lanes are always serial).
max_inflight is a DIAGNOSTIC lever, NOT sizing/capacity guidance. NOT a Ray Serve /
real-I/O / inference / cluster / capacity / latency-SLO / performance claim, NOT "HPX
beats Ray" / "RayX makes Ray faster", and does NOT recommend making non-blocking the
default. No W=32 scaling curve; no NUMA/binding; no priorities/pools/counters.

Usage:
    python .../run_ray_hosting_rayx_nonblocking_residual.py            # --smoke (default)
    python .../run_ray_hosting_rayx_nonblocking_residual.py --full
    python .../run_ray_hosting_rayx_nonblocking_residual.py --full --probe a
    python .../run_ray_hosting_rayx_nonblocking_residual.py --full --probe b --best-mi 16
    python .../run_ray_hosting_rayx_nonblocking_residual.py --full --no-mi32
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

# busy_sum checkpoint stride (mirrors BUSY_SUM_STRIDE in runtime_ops.hpp); used only to
# report the derived checkpoint_count of the compute op.
BUSY_SUM_STRIDE = 8192

# Synthetic parked-wait duration for the Wp class (echoed back as the op's value).
# Single-chunk (< PARK_MS_STRIDE = 10 ms). Synthetic cooperative wait, NOT real I/O.
PARK_MS = 5

# p99 retention is only emitted when the pooled per-arm C sample count reaches this
# floor; otherwise NA. p50/p90 retention always reported (observational, DIRECTIONAL).
P99_MIN_SAMPLES = 100

# Cap on K_Wp from calibration, so timer/rescheduling overhead of an absurd parked count
# cannot masquerade as "no overlap". Recalibrated per granularity in Probe B.
K_WP_CAP = 400

# Reading thresholds (READING criteria only; NEVER structural gates).
RET_THR_SUPPORT = 0.90    # C throughput retention >= this = compute intact (SUPPORT)
RET_THR_STOP = 0.70       # C throughput retention <= this = eroded (positive control)
RET_P90_SUPPORT = 1.20    # C p90 retention <= this = latency intact (directional)

# Compute-baseline-flatness guard: compute-only wall_C should stay within this fractional
# band across modes/max_inflight at fixed hpx_threads, else the ladder is INCONCLUSIVE
# (mode/cap may be changing compute capacity / driver behaviour).
WALL_C_FLAT_TOL = 0.25

# qd_mean_T attribution bands (READING only). At/below DRAINED -> the lane queue drained;
# at/above BACKED_UP -> the cap is still binding. Between -> ambiguous (lean on trend).
QD_DRAINED_MAX = 1.0
QD_BACKED_UP = 3.0

# "Materially shrinks" for the Probe B granularity split, and the minimum cap-extension
# retention rise to call a trend a recovery rather than a plateau.
RET_RISE_MIN = 0.10

# How often (in retire iterations) to sample lane_stats during an arm.
LANE_SAMPLE_STRIDE = 3

# ---- smoke-mode parameters (laptop-safe DEFAULT; SMOKE-ONLY, not evidence) ----------
HT_A_SMOKE = (4,)               # Probe A: num_lanes == hpx_threads
LEVELS_SMOKE = ("near", "over")
MI_SWEEP_SMOKE = (4, 8)         # non-blocking max_inflight sweep
MI_GUARD_SMOKE = 16             # optional guarded cell, run LAST
KC_SMOKE = 12
REPS_SMOKE = 2
WARMUP_SMOKE = 1
GRAN_A_SMOKE = ("fine", 500_000)
HT_B_SMOKE = 4                  # Probe B HT (over only)
GRAN_B_SMOKE = (("fine", 500_000), ("coarse", 2_000_000))
BEST_MI_FALLBACK_SMOKE = 8

# ---- full-mode parameters (homogeneous many-core Linux; observation-only) -----------
HT_A_FULL = (4, 8)
LEVELS_FULL = ("near", "over")
MI_SWEEP_FULL = (4, 8, 16)      # design sweep; mi=1 overhead floor was exp37, not here
MI_GUARD_FULL = 32              # optional guarded decisive cell, run LAST
KC_FULL = 60                    # reps=3 -> pooled C >= 180 so p99 retention is eligible
REPS_FULL = 3
WARMUP_FULL = 1
GRAN_A_FULL = ("fine", 2_000_000)
HT_B_FULL = 8                   # Probe B HT (over only; the worst exp37 residual)
GRAN_B_FULL = (("fine", 2_000_000), ("coarse", 20_000_000))
BEST_MI_FALLBACK_FULL = 16      # used only if Probe B runs standalone without Probe A


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
    """Closed-loop window relative to hpx_threads (== num_lanes here). over = 4*ht is the
    offered concurrency O cited in the design; held constant across the serial-vs-
    nonblocking comparison so the only intended lever is op-lane dispatch + max_inflight."""
    if level == "under":
        return max(1, ht // 2)
    if level == "near":
        return ht
    return 4 * ht  # over -> O = 4*HT


def _mode_label(nonblocking, max_inflight):
    return f"nb-mi{max_inflight}" if nonblocking else "serial"


def _mode_sort_key(cell):
    # serial first, then non-blocking by ascending max_inflight.
    return (-1, 0) if not cell["nonblocking"] else (1, cell["max_inflight"])


# --------------------------------------------------------------------------- #
# Ray actor (one per cell; hosts ONE Runtime). Reused verbatim from exp37 --   #
# no C++ change, no schema change; exp38 only varies the existing knobs.       #
# --------------------------------------------------------------------------- #
def _build_actor(ray):
    """Define the Ray actor inside a function so this module imports without Ray present
    (clean-skip path)."""

    @ray.remote
    class RayxModeActor:
        """Hosts ONE rayx.runtime.Runtime(num_lanes=NL, hpx_threads=HT) in either the
        default SERIAL op-lane mode or the experimental NON-BLOCKING op-lane mode
        (experimental_nonblocking_op_lanes=True, max_inflight_per_lane=MI). One actor per
        (HT, mode, MI) cell -- the mode/max_inflight are constructor-fixed and the HPX
        runtime is process-global, so a fresh process per cell is required. RuntimeFuture
        / OperationResult are created and retired INSIDE this actor; only plain
        scalars/lists/dicts cross the Ray boundary."""

        def __init__(self, num_lanes, hpx_threads, nonblocking, max_inflight, rayx_src):
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
            """Run one arm. `classes` maps class label -> {op_id, arg, count, window}.
            Holds each class's concurrency at its window, refilling as ops retire. Retires
            by READINESS (order-agnostic). Returns ONLY plain rows."""
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
# Measurement helpers (reused verbatim from exp37)                            #
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
    """Run one arm `reps` times (after `warmup`). Aggregates per-call walls, pools C-class
    total_ms across reps, checks structural gates, and OR/means the lane trace. Returns a
    plain summary dict."""
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


# --------------------------------------------------------------------------- #
# One ladder (serial anchor + a set of modes at fixed ht/level/granularity)    #
# --------------------------------------------------------------------------- #
def _run_one_ladder(ray, RayxModeActor, gates, ht, nl, gran_name, n_c, level, modes,
                    k_c, reps, warmup, k_wp=None):
    """Run the given `modes` (list of (nonblocking, max_inflight)) at one
    (ht, nl, gran, level). If k_wp is None, the FIRST mode must be the serial anchor; K_Wp
    is calibrated from its compute-only wall_C and held across the rest. Returns
    (cells, k_wp)."""
    o = outstanding_for(level, ht)
    cells = []
    for nonblocking, mi in modes:
        actor = RayxModeActor.options(num_cpus=ht).remote(
            nl, ht, nonblocking, (mi if mi is not None else 1), RAYX_SRC)
        ids = ray.get(actor.lane_ids.remote())
        if len(ids) != nl or not all(
                isinstance(i, str) and i.startswith("rt-hpx-") for i in ids):
            gates.lane_ids_ok = False
        arm_c = _measure_arm(
            ray, actor,
            {"C": {"op_id": "busy_sum", "arg": n_c, "count": k_c, "window": o}},
            reps, warmup, gates)
        wall_C = arm_c["wall_compute_med"]
        if k_wp is None:  # calibrate from the serial baseline wall_C (first mode)
            k_wp_1x = int(round(wall_C * o / PARK_MS)) if PARK_MS > 0 else o
            k_wp = max(ht, min(K_WP_CAP, k_wp_1x))
        arm_wp = _measure_arm(
            ray, actor,
            {"Wp": {"op_id": "park_ms", "arg": PARK_MS, "count": k_wp, "window": o}},
            reps, warmup, gates)
        arm_t = _measure_arm(
            ray, actor,
            {"C": {"op_id": "busy_sum", "arg": n_c, "count": k_c, "window": o},
             "Wp": {"op_id": "park_ms", "arg": PARK_MS, "count": k_wp, "window": o}},
            reps, warmup, gates)
        cells.append(_cell_metrics(
            ht, nl, gran_name, n_c, level, o, k_c, k_wp, reps,
            nonblocking, mi, arm_c, arm_wp, arm_t))
        try:
            ray.get(actor.shutdown.remote())
        except Exception:
            gates.clean_shutdown = False
        ray.kill(actor)
    return cells, k_wp


def _cell_metrics(ht, nl, gran, n_c, level, o, k_c, k_wp, reps,
                  nonblocking, max_inflight, arm_c, arm_wp, arm_t):
    """Reduce one cell to a plain reading dict."""
    wall_C = arm_c["wall_compute_med"]
    wall_Wp = arm_wp["wall_total_med"]
    wall_CWp = arm_t["wall_compute_med"]
    wall_CWp_total = arm_t["wall_total_med"]

    thr_C_ref = (k_c / (wall_C / 1e3)) if wall_C > 0 else float("nan")
    thr_C_test = (k_c / (wall_CWp / 1e3)) if wall_CWp > 0 else float("nan")
    thr_retention = (thr_C_test / thr_C_ref) if thr_C_ref > 0 else float("nan")

    cC, cT = arm_c["pooled_C"], arm_t["pooled_C"]
    n_samples = min(len(cC), len(cT))
    # Approximate per-op compute latency (INCLUDES queue wait under window O); used only
    # as an APPROXIMATE reading aid in the saturation calc, never as a gate.
    s_C_ms = statistics.median(cC) if cC else float("nan")

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
        "s_C_ms": s_C_ms,
        "thr_retention": thr_retention,
        "p50_ret": ret(50), "p90_ret": ret(90), "p99_ret": p99_ret,
        "n_samples_C": n_samples,
        "qd_mean_C": arm_c["lane_trace"]["qd_mean"],
        "qd_mean_T": arm_t["lane_trace"]["qd_mean"],
        "qd_max_T": arm_t["lane_trace"]["qd_max"],
        "lane_trace_T": arm_t["lane_trace"],
    }


# --------------------------------------------------------------------------- #
# Probe A / Probe B drivers (share one already-initialised ray + actor)        #
# --------------------------------------------------------------------------- #
def _probe_a_cells(ray, RayxModeActor, gates, ht_values, n_c, levels, mi_sweep,
                   mi_guard, k_c, reps, warmup):
    """Probe A: per (ht, level) a serial anchor + non-blocking mi_sweep, then (LAST) an
    optional guarded mi_guard cell reusing the ladder's calibrated K_Wp."""
    gran_name = "fine"
    cells = []
    kwp_by_ladder = {}
    # Phase 1 -- serial + nb(mi_sweep); calibrate and remember K_Wp per (ht, level).
    for ht in ht_values:
        nl = ht  # num_lanes == hpx_threads (the exp35/exp36/exp37 eroded config)
        for level in levels:
            modes = [(False, None)] + [(True, mi) for mi in mi_sweep]
            lc, kwp = _run_one_ladder(
                ray, RayxModeActor, gates, ht, nl, gran_name, n_c, level, modes,
                k_c, reps, warmup, k_wp=None)
            cells += lc
            kwp_by_ladder[(ht, level)] = kwp
    # Phase 2 (guarded, run LAST) -- mi_guard for every ladder, reusing calibrated K_Wp.
    if mi_guard:
        for ht in ht_values:
            nl = ht
            for level in levels:
                lc, _ = _run_one_ladder(
                    ray, RayxModeActor, gates, ht, nl, gran_name, n_c, level,
                    [(True, mi_guard)], k_c, reps, warmup,
                    k_wp=kwp_by_ladder[(ht, level)])
                cells += lc
    return cells


def _probe_b_cells(ray, RayxModeActor, gates, ht, grans, best_mi, k_c, reps, warmup):
    """Probe B: HT over only; per granularity a serial anchor + nb(best_mi). K_Wp is
    RECALIBRATED per granularity (each granularity's serial anchor calibrates its own)."""
    nl = ht
    level = "over"
    cells = []
    for gran_name, n_c in grans:
        modes = [(False, None), (True, best_mi)]
        lc, _ = _run_one_ladder(
            ray, RayxModeActor, gates, ht, nl, gran_name, n_c, level, modes,
            k_c, reps, warmup, k_wp=None)
        cells += lc
    return cells


def _select_best_mi(a_cells, prefer_ht):
    """Best max_inflight from Probe A's OVER ladder at prefer_ht (fall back to the largest
    HT present): the non-blocking cell with the HIGHEST thr_retention (tie -> smallest
    mi, the cheaper cap). Returns int or None."""
    over_nb = [c for c in a_cells if c["nonblocking"] and c["level"] == "over"]
    if not over_nb:
        return None
    hts = sorted({c["ht"] for c in over_nb})
    ht = prefer_ht if prefer_ht in hts else hts[-1]
    cand = [c for c in over_nb if c["ht"] == ht
            and c["thr_retention"] == c["thr_retention"]]  # drop NaN
    if not cand:
        cand = [c for c in over_nb if c["ht"] == ht]
        return min(cand, key=lambda c: c["max_inflight"])["max_inflight"] if cand else None
    best = max(cand, key=lambda c: (c["thr_retention"], -c["max_inflight"]))
    return int(best["max_inflight"])


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
    """Drop any hpx_threads cell that exceeds the local cpu_count; num_lanes ==
    hpx_threads here."""
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


def _print_cell_line(c):
    anchor = " (serial anchor)" if not c["nonblocking"] else ""
    print(f"    mode={c['mode_label']:<8}{anchor:<16} "
          f"wall_C={_fmt(c['wall_C'],'.1f')} wall_Wp={_fmt(c['wall_Wp'],'.1f')} "
          f"wall_CWp={_fmt(c['wall_CWp'],'.1f')} (total="
          f"{_fmt(c['wall_CWp_total'],'.1f')}) ms")
    print(f"        thr_retention={_fmt(c['thr_retention'])}  "
          f"p50/p90/p99 ret={_fmt(c['p50_ret'])}/{_fmt(c['p90_ret'])}/"
          f"{_fmt(c['p99_ret'])} (Cn={c['n_samples_C']}; tail DIRECTIONAL)")
    print(f"        qd_mean[C={_fmt(c['qd_mean_C'],'.1f')} "
          f"T={_fmt(c['qd_mean_T'],'.1f')}] qd_max_T={c['qd_max_T']} "
          f"lane_stats_T[backlog={c['lane_trace_T']['backlog_seen']} "
          f"active_frac={_fmt(c['lane_trace_T']['active_fraction'])}]")


def _print_probe_a_table(cells):
    print("PROBE A -- serial anchor + non-blocking max_inflight sweep "
          "(fine, num_lanes==hpx_threads); guarded mi cell is run LAST:")
    last_key = None
    for c in sorted(cells, key=lambda x: (x["ht"], x["level"], _mode_sort_key(x))):
        key = (c["ht"], c["level"])
        if key != last_key:
            print(f"  hpx_threads={c['ht']}  num_lanes={c['nl']}  load={c['level']} "
                  f"outstanding(O)={c['outstanding']}  granularity={c['gran']} "
                  f"(n_c={c['n_c']}, cpc={checkpoint_count(c['n_c'])})  "
                  f"K_C={c['k_c']} K_Wp={c['k_wp']} reps={c['reps']}")
            last_key = key
        _print_cell_line(c)
    print()


def _print_probe_b_table(cells, best_mi):
    print(f"PROBE B -- HT=8 over only, best max_inflight={best_mi}; fine vs coarse "
          f"(K_Wp RECALIBRATED per granularity):")
    last_key = None
    for c in sorted(cells, key=lambda x: (x["n_c"], _mode_sort_key(x))):
        key = (c["gran"], c["n_c"])
        if key != last_key:
            print(f"  granularity={c['gran']} (n_c={c['n_c']}, "
                  f"cpc={checkpoint_count(c['n_c'])})  hpx_threads={c['ht']} "
                  f"num_lanes={c['nl']} load={c['level']} outstanding(O)={c['outstanding']}"
                  f"  K_C={c['k_c']} K_Wp={c['k_wp']} reps={c['reps']}")
            last_key = key
        _print_cell_line(c)
    print()


def _print_saturation_calc(a_cells):
    """Pre-run-style compute-demand / saturation SANITY CALC (READING AID, not a gate),
    computed from Probe A serial anchors. At over O=4*HT>HT, so C-only compute is
    worker-bound BY CONSTRUCTION; an H-sat residual is MIXED pressure beyond that."""
    print("SATURATION SANITY CALC (reading aid, NOT a gate; from the serial anchors):")
    serials = sorted([c for c in a_cells if not c["nonblocking"]],
                     key=lambda c: (c["ht"], c["level"]))
    if not serials:
        print("  (no serial anchors available)")
        print()
        return
    for c in serials:
        ht, o, k_c = c["ht"], c["outstanding"], c["k_c"]
        wall_C, s_C = c["wall_C"], c["s_C_ms"]
        thr_C = (k_c / (wall_C / 1e3)) if wall_C and wall_C > 0 else float("nan")
        # Approximate achieved compute concurrency (s_C includes queue wait -> upper-ish).
        demand = (k_c * s_C / wall_C) if (wall_C and wall_C > 0 and s_C == s_C) \
            else float("nan")
        over_subscribed = o > ht
        print(f"  HT={ht} {c['level']}: O={o}  C-only wall_C={_fmt(wall_C,'.1f')}ms "
              f"thr_C={_fmt(thr_C,'.1f')} op/s  s_C~={_fmt(s_C,'.2f')}ms (approx, incl "
              f"queue)  demand~={_fmt(demand,'.1f')}")
        if over_subscribed:
            print(f"      O={o} > HT={ht}: compute offered ABOVE worker capacity -> "
                  f"C-only is worker-bound by construction (only HT run at once).")
            print(f"      prediction: raising max_inflight drains the lane queue (qd) but "
                  f"migrates excess compute into the INVISIBLE HPX scheduler queue ->")
            print(f"      expect qd->~0 with a retention PLATEAU (H-sat signature) even "
                  f"with zero retirement cost; Probe B (coarsen) splits H-sat vs H-retire.")
        else:
            print(f"      O={o} <= HT={ht}: compute offered within worker capacity -> "
                  f"H-sat less likely here; a plateau points to H-retire/H-driver.")
    print()


# ---- attribution readings (READING criteria only; NEVER gates) ------------- #
def _ladder_controls(ladder, tag):
    """Shared controls. Returns an INCONCLUSIVE (verdict, msg) if a control fails, else
    None. (serial-erosion is checked by the caller because its meaning differs by use.)"""
    # Compute-baseline flatness across modes.
    wall_cs = [c["wall_C"] for c in ladder if c["wall_C"] > 0]
    if wall_cs:
        wmin, wmax = min(wall_cs), max(wall_cs)
        if wmin > 0 and (wmax - wmin) / wmin > WALL_C_FLAT_TOL:
            curve = " ".join(f"{c['mode_label']}:{_fmt(c['wall_C'],'.1f')}ms"
                             for c in sorted(ladder, key=_mode_sort_key))
            return ("INCONCLUSIVE",
                    f"[{tag}] compute-only wall_C is NOT flat across modes "
                    f"(>{WALL_C_FLAT_TOL:.0%}: {curve}): the mode/cap may be changing "
                    "compute capacity or driver behaviour; attribution unsafe.")
    # Driver / lane_stats: matched arms must be backlogged/active.
    backlog_any = any(c["lane_trace_T"]["backlog_seen"] for c in ladder)
    active_mean = statistics.fmean([c["lane_trace_T"]["active_fraction"] for c in ladder])
    if not backlog_any and active_mean < 0.1:
        return ("INCONCLUSIVE",
                f"[{tag}] lane_stats shows HPX not backlogged/active in the matched arms "
                "(driver-governed): wall indicators measure the CPython driver, not the "
                "scheduler. Raise K / windows.")
    return None


def _interpret_probe_a_ladder(ladder):
    """Attribution for ONE (hpx_threads, level) ladder. NEAR is the FULL control (H-cap
    should resolve at low mi); OVER is the attribution target. Returns (verdict, msg)."""
    serial = next((c for c in ladder if not c["nonblocking"]), None)
    nbs = sorted([c for c in ladder if c["nonblocking"]], key=lambda c: c["max_inflight"])
    if serial is None or not nbs:
        return ("INCONCLUSIVE", "ladder missing the serial anchor or non-blocking sweep.")
    ht, level = serial["ht"], serial["level"]
    tag = f"hpx_threads={ht} num_lanes={ht} load={level}"
    is_target = (level == "over")

    ctrl = _ladder_controls(ladder, tag)
    if ctrl is not None:
        return ctrl

    rs = serial["thr_retention"]
    seq = (f"serial={_fmt(rs)} -> " +
           " -> ".join(f"{c['mode_label']}={_fmt(c['thr_retention'])}" for c in nbs))
    top = nbs[-1]
    qd_base, qd_top = nbs[0]["qd_mean_T"], top["qd_mean_T"]
    ret_top = top["thr_retention"]
    qd_trend = (f"qd_mean_T {_fmt(qd_base,'.1f')}->{_fmt(qd_top,'.1f')} "
                f"(qd_max_T base/top {nbs[0]['qd_max_T']}/{top['qd_max_T']})")
    nb_rets = [c["thr_retention"] for c in nbs]
    monotone = all(nb_rets[i + 1] >= nb_rets[i] - 0.03 for i in range(len(nb_rets) - 1))
    # Guarded/top cell = the highest-max_inflight nb cell (nbs is sorted ascending);
    # compare it to the previous nb cell for churn/knee reporting. Derived from the
    # ACTUAL ladder so it works for any sweep (smoke guard mi=16 as well as full mi=32).
    guard_note = ""
    if len(nbs) >= 2:
        guard, prev = top, nbs[-2]
        dr = (guard["thr_retention"] - prev["thr_retention"])
        guard_note = (f" guarded/top {guard['mode_label']}: "
                      f"d_retention={_fmt(dr)} vs {prev['mode_label']}, "
                      f"qd_mean_T={_fmt(guard['qd_mean_T'],'.1f')}"
                      + (" [CHURN/KNEE: retention dropped at the guarded cap]"
                         if dr < -RET_RISE_MIN else ""))

    qd_drained = (qd_top == qd_top and qd_top <= QD_DRAINED_MAX)
    qd_backed_up = (qd_top == qd_top and qd_top >= QD_BACKED_UP)
    ret_support = (ret_top == ret_top and ret_top >= RET_THR_SUPPORT)

    # NEAR -- the FULL control: H-cap should resolve (qd->0 and retention -> SUPPORT).
    if not is_target:
        if qd_drained and ret_support:
            return ("H-CAP (near FULL control holds)",
                    f"[{tag}] {seq}; {qd_trend}: the lane queue drained and retention "
                    f"reached SUPPORT (top {top['mode_label']}={_fmt(ret_top)}). NEAR "
                    "control holds -- H-cap resolves at low max_inflight as expected." +
                    guard_note)
        return ("INCONCLUSIVE (near control weak)",
                f"[{tag}] {seq}; {qd_trend}: NEAR did NOT cleanly reach qd->0 + SUPPORT "
                f"(top={_fmt(ret_top)}). The FULL control is weak, so the OVER "
                "attribution is read with caution." + guard_note)

    # OVER -- the attribution target. Positive control: serial must erode.
    if not (rs == rs and rs <= RET_THR_STOP):
        return ("INCONCLUSIVE (serial did not erode)",
                f"[{tag}] serial anchor did NOT reproduce erosion (thr_retention="
                f"{_fmt(rs)} > {RET_THR_STOP:g}): no residual to attribute here.")

    if qd_backed_up:
        return ("STILL CAP-BOUND",
                f"[{tag}] {seq}; {qd_trend}: the lane queue stays backed up and retention "
                f"stalls (top {top['mode_label']}={_fmt(ret_top)}). The cap is still "
                "binding (the sweep did not reach the cap the saturation calc needs, e.g. "
                "max_inflight < O) or the consumer/retire path is throughput-bound -- "
                "extend max_inflight (the guarded mi=32 cell) before attributing cost." +
                guard_note)
    if qd_drained and ret_support:
        return ("H-CAP",
                f"[{tag}] {seq}; {qd_trend}: extending max_inflight drained the lane queue "
                f"AND lifted retention to SUPPORT (top {top['mode_label']}={_fmt(ret_top)},"
                f" p90 {_fmt(top['p90_ret'])} directional). The exp37 PARTIAL was an "
                "UNDERSIZED CAP; the non-blocking lane removes head-of-line given enough "
                "in-flight slots. No retirement-path optimization is motivated yet." +
                guard_note)
    if qd_drained and not ret_support:
        return ("DRAINED-BUT-PLATEAU (H-retire / H-sat / H-driver)",
                f"[{tag}] {seq}; {qd_trend}: the lane queue drained but retention plateaus "
                f"BELOW SUPPORT (top {top['mode_label']}={_fmt(ret_top)}). The residual is "
                "retirement-path (H-retire), mixed scheduler/worker saturation beyond the "
                "C-only baseline (H-sat), or CPython driver cadence (H-driver). Run Probe B"
                " -- H-retire SHRINKS with coarser compute; H-sat/H-driver do NOT." +
                guard_note)
    if not monotone:
        return ("INCONCLUSIVE (non-monotone)",
                f"[{tag}] {seq}; {qd_trend}: the cap-extension trend is non-monotone "
                "without a clean qd->0 crossing -- cannot cleanly partition the residual."
                + guard_note)
    return ("INCONCLUSIVE (ambiguous qd/retention)",
            f"[{tag}] {seq}; {qd_trend}: qd and retention move inconsistently (qd not "
            f"clearly drained and retention not at SUPPORT, top={_fmt(ret_top)}). "
            "Inconclusive partition." + guard_note)


def _interpret_probe_b(b_cells, best_mi):
    """Split H-retire (granularity-sensitive) from H-sat/H-driver (granularity-insensitive)
    at HT=8 over: compare the residual (gap to SUPPORT, at qd~0) fine vs coarse. Positive
    serial-erosion control is checked PER granularity (coarse may not erode -- exp35)."""
    by_gran = {}
    for c in b_cells:
        by_gran.setdefault(c["gran"], {})[
            "serial" if not c["nonblocking"] else "nb"] = c
    if "fine" not in by_gran or "coarse" not in by_gran:
        return ("INCONCLUSIVE", "Probe B missing a granularity (need fine + coarse).")

    notes = []
    resid = {}
    qd_ok = {}
    for g in ("fine", "coarse"):
        pair = by_gran[g]
        sc, nb = pair.get("serial"), pair.get("nb")
        if sc is None or nb is None:
            return ("INCONCLUSIVE", f"Probe B {g} missing serial or nb(best) cell.")
        eroded = (sc["thr_retention"] == sc["thr_retention"]
                  and sc["thr_retention"] <= RET_THR_STOP)
        r = nb["thr_retention"]
        resid[g] = (RET_THR_SUPPORT - r) if r == r else float("nan")
        # Probe B is decisive only after qd~0: while the lane is still (partially)
        # cap-bound the residual is not yet in the drained-plateau regime Probe B splits.
        qd_ok[g] = (nb["qd_mean_T"] == nb["qd_mean_T"]
                    and nb["qd_mean_T"] <= QD_DRAINED_MAX)
        notes.append(
            f"{g}: serial={_fmt(sc['thr_retention'])}{'' if eroded else ' (NOT eroded'}"
            f"{'' if eroded else '; positive control absent -- exp35)'} "
            f"nb-mi{best_mi}={_fmt(r)} residual={_fmt(resid[g])} "
            f"qd_mean_T={_fmt(nb['qd_mean_T'],'.1f')}")
    msg_tail = "; ".join(notes)

    if not (qd_ok["fine"] and qd_ok["coarse"]):
        return ("INCONCLUSIVE",
                f"[HT=8 over] Probe B is decisive only after qd~0 (qd_mean_T <= "
                f"{QD_DRAINED_MAX:g}); this granularity is still not fully drained, so the "
                f"residual is not yet in the drained-plateau regime Probe B splits. "
                f"{msg_tail}.")
    rf, rcq = resid["fine"], resid["coarse"]
    if not (rf == rf and rcq == rcq):
        return ("INCONCLUSIVE", f"[HT=8 over] residual unavailable. {msg_tail}.")

    if rcq <= rf - RET_RISE_MIN or rcq <= (1.0 - RET_THR_SUPPORT):
        return ("H-RETIRE",
                f"[HT=8 over] the residual SHRINKS with coarser compute "
                f"(fine={_fmt(rf)} -> coarse={_fmt(rcq)}): per-op retirement-path cost "
                "dominates (continuation / per-lane mutex serialization + lock-acquire "
                "wait / handoff / in-flight accounting), since dropping the per-op "
                f"retire rate ~10x recovered retention. {msg_tail}.")
    if abs(rcq - rf) < RET_RISE_MIN:
        return ("H-SAT / H-DRIVER",
                f"[HT=8 over] the residual PERSISTS with coarser compute "
                f"(fine={_fmt(rf)} -> coarse={_fmt(rcq)}): NOT per-op retirement cost. "
                "It is mixed scheduler/worker saturation beyond the C-only baseline "
                "(H-sat) or CPython driver cadence (H-driver) -- counter-free probes "
                f"cannot split these further (Probe C, deferred/out-of-scope). {msg_tail}.")
    return ("INCONCLUSIVE",
            f"[HT=8 over] the residual moved but not cleanly (fine={_fmt(rf)} -> "
            f"coarse={_fmt(rcq)}); granularity split is ambiguous. {msg_tail}.")


def _print_reading_caveats():
    print("  COUNTER-FREE attribution: exp38 uses only the existing max_inflight_per_lane "
          "knob; it adds NO C++ and NO counters and does NOT optimize notify_all / mutex /"
          " inflight bookkeeping. notify_all is NOT treated as a thundering herd (one "
          "consumer waiter, plus possibly one shutdown waiter); the plausible retirement "
          "cost is per-lane mutex serialization / lock-acquire wait / handoff / in-flight "
          "accounting (Probe C counters, if ever needed, are deferred and out of scope).")
    print("  Retention is NORMALIZED to the C-only baseline at the SAME hpx_threads, so "
          "finite HT ALONE is never the attribution -- H-sat means MIXED-workload "
          "scheduler/worker pressure BEYOND that baseline.")
    print("  qd_mean_T is the crux; p50/p90/p99 retention are DIRECTIONAL (reps small). "
          "The guarded mi=32 cell (HT=4 and HT=8) is run LAST; if it shows churn/a knee, "
          "HT=4 (sweep reaches mi=32 = 2*O) is the decisive H-cap cell and HT=8 is "
          "corroborating.")
    print("  Non-blocking is EXPERIMENTAL and operation-lane-only (actor lanes stay "
          "serial). max_inflight is a DIAGNOSTIC lever, NOT sizing/capacity guidance. "
          "park_ms is SYNTHETIC cooperative parked wait (NOT real I/O, NOT inference). "
          "Observation-only, synthetic, machine-specific. Not Ray Serve, not cluster "
          "scaling, not HPX priority scheduling, not NUMA/binding, not a latency-SLO / "
          "capacity / performance claim, NOT 'HPX beats Ray' / 'RayX makes Ray faster', "
          "and NOT a recommendation to make non-blocking the default. No W=32 scaling "
          "curve; no priorities/pools/counters.")
    print()


def _print_reading(a_cells, b_cells, best_mi, mode):
    if mode == "smoke":
        print("READING (observation-only) [INCONCLUSIVE (smoke-only)]: smoke validates "
              "the Probe A cap-extension sweep + guarded cell, the Probe B fine/coarse "
              "split with per-granularity K_Wp recalibration, the three-arm / calibration "
              "/ lane_stats structure, order-agnostic retirement, and the saturation calc "
              "plumbing only -- sample counts are low and this is NOT evidence. Run --full "
              "on a homogeneous many-core Linux node for an observation.")
        print()
        _print_reading_caveats()
        return

    if a_cells:
        _print_saturation_calc(a_cells)
        print("READING (observation-only, this run/machine) -- PROBE A per-ladder "
              "attribution (the counter-free qd->0 partition):")
        keys = sorted({(c["ht"], c["level"]) for c in a_cells})
        for k in keys:
            ladder = [c for c in a_cells if (c["ht"], c["level"]) == k]
            verdict, msg = _interpret_probe_a_ladder(ladder)
            print(f"  [{verdict}] {msg}")
        print()
    if b_cells:
        print(f"READING (observation-only) -- PROBE B granularity split at HT=8 over "
              f"(best max_inflight={best_mi}):")
        verdict, msg = _interpret_probe_b(b_cells, best_mi)
        print(f"  [{verdict}] {msg}")
        print("  Probe B is decisive only when Probe A's HT=8 over landed in "
              "DRAINED-BUT-PLATEAU; if Probe A attributed H-cap or stayed cap-bound, "
              "read Probe B as corroborating context only.")
        print()
    _print_reading_caveats()


# --------------------------------------------------------------------------- #
# Orchestration                                                                #
# --------------------------------------------------------------------------- #
def _run_experiment(params):
    """Init Ray once, run the selected probe(s), tear down. Returns
    (a_cells, b_cells, best_mi, gates_dict, skipped, reason)."""
    gates = _Gates()
    try:
        import ray  # noqa: F401
    except Exception as e:
        return [], [], None, gates.as_dict(), True, \
            f"ray unavailable: {type(e).__name__}: {e}"
    try:
        from rayx.runtime import Runtime  # noqa: F401
    except Exception as e:
        return [], [], None, gates.as_dict(), True, \
            f"rayx.runtime unavailable: {type(e).__name__}: {e}"

    import ray
    ht_a = params["ht_a"]
    ht_b = params["ht_b"]
    probe = params["probe"]
    max_ht = max(list(ht_a) + [ht_b])
    ray.init(num_cpus=max_ht + 1, ignore_reinit_error=True,
             log_to_driver=False, configure_logging=False)
    RayxModeActor = _build_actor(ray)
    a_cells, b_cells = [], []
    best_mi = params["best_mi_override"]
    try:
        if probe in ("a", "both"):
            a_cells = _probe_a_cells(
                ray, RayxModeActor, gates, ht_a, params["n_c_a"], params["levels"],
                params["mi_sweep"], params["mi_guard"], params["k_c"], params["reps"],
                params["warmup"])
        if probe in ("b", "both"):
            if best_mi is None and a_cells:
                best_mi = _select_best_mi(a_cells, prefer_ht=ht_b)
            if best_mi is None:
                best_mi = params["best_mi_fallback"]
            b_cells = _probe_b_cells(
                ray, RayxModeActor, gates, ht_b, params["grans_b"], best_mi,
                params["k_c"], params["reps"], params["warmup"])
    finally:
        ray.shutdown()
    return a_cells, b_cells, best_mi, gates.as_dict(), False, None


def _run_and_print(mode, probe, best_mi_override, include_guard):
    cpu = os.cpu_count()
    if mode == "smoke":
        ht_a, warn = _cap_threads(HT_A_SMOKE, cpu)
        ht_b = _cap_threads((HT_B_SMOKE,), cpu)[0][0]
        params = {
            "probe": probe, "ht_a": tuple(ht_a), "ht_b": ht_b,
            "n_c_a": GRAN_A_SMOKE[1], "levels": LEVELS_SMOKE,
            "mi_sweep": MI_SWEEP_SMOKE,
            "mi_guard": (MI_GUARD_SMOKE if include_guard else None),
            "grans_b": GRAN_B_SMOKE, "k_c": KC_SMOKE, "reps": REPS_SMOKE,
            "warmup": WARMUP_SMOKE, "best_mi_override": best_mi_override,
            "best_mi_fallback": BEST_MI_FALLBACK_SMOKE,
        }
        title = ("exp38 non-blocking op-lane RESIDUAL ATTRIBUTION (SMOKE) -- structural "
                 "validation only; SMOKE-ONLY, not evidence")
    else:
        ht_a, warn = _cap_threads(HT_A_FULL, cpu)
        ht_b = _cap_threads((HT_B_FULL,), cpu)[0][0]
        params = {
            "probe": probe, "ht_a": tuple(ht_a), "ht_b": ht_b,
            "n_c_a": GRAN_A_FULL[1], "levels": LEVELS_FULL,
            "mi_sweep": MI_SWEEP_FULL,
            "mi_guard": (MI_GUARD_FULL if include_guard else None),
            "grans_b": GRAN_B_FULL, "k_c": KC_FULL, "reps": REPS_FULL,
            "warmup": WARMUP_FULL, "best_mi_override": best_mi_override,
            "best_mi_fallback": BEST_MI_FALLBACK_FULL,
        }
        title = ("exp38 non-blocking op-lane RESIDUAL ATTRIBUTION (FULL) -- homogeneous "
                 "many-core Linux; observation-only, machine-specific")

    print(title)
    _print_machine_info()
    print("NOTE -- attribute the exp37 OVER-load residual with the COUNTER-FREE qd->0")
    print("  partition (existing max_inflight_per_lane knob only). Probe A: serial +")
    print("  non-blocking max_inflight sweep at fixed num_lanes==hpx_threads, plus a")
    print("  guarded mi cell run LAST. Probe B: HT=8 over, fine vs coarse, to split")
    print("  H-retire (granularity-sensitive) from H-sat/H-driver (not). No C++, no")
    print("  counters; notify_all/mutex/inflight bookkeeping NOT optimized. park_ms is")
    print("  synthetic (not I/O). Experimental, op-lanes only; max_inflight is diagnostic.")
    if mode != "smoke" and not sys.platform.startswith("linux"):
        print("  On Mac/laptop/heterogeneous hardware this --full output is SMOKE-ONLY, "
              "not evidence.")
    if warn:
        print(f"  {warn}")
    print(f"  probe={probe} hpx_threads_A(=num_lanes)={list(params['ht_a'])} "
          f"max_inflight_sweep={list(params['mi_sweep'])} "
          f"guard_mi={params['mi_guard']} levels={list(params['levels'])} "
          f"HT_B={params['ht_b']} K_C={params['k_c']} park_ms={PARK_MS} "
          f"reps={params['reps']} warmup={params['warmup']} "
          f"p99_min_samples={P99_MIN_SAMPLES}")
    print()

    a_cells, b_cells, best_mi, gates, skipped, reason = _run_experiment(params)
    if skipped:
        print(f"SKIP: {reason}")
        return 0

    if a_cells:
        _print_probe_a_table(a_cells)
    if b_cells:
        _print_probe_b_table(b_cells, best_mi)
    gates_ok = all(gates.values())
    print("  gates: " + ", ".join(f"{k}={v}" for k, v in sorted(gates.items())))
    print()
    _print_reading(a_cells, b_cells, best_mi, mode)

    if gates_ok:
        print("STRUCTURAL GATES: PASS")
        return 0
    print("STRUCTURAL GATES: FAIL")
    return 1


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--smoke", action="store_true",
                    help="structural smoke (the default): tiny Probe A + Probe B; "
                         "SMOKE-ONLY")
    ap.add_argument("--full", action="store_true",
                    help="Probe A (HT in {4,8}, near/over, mi in {4,8,16}+guard 32) and "
                         "Probe B (HT=8 over, fine vs coarse); homogeneous many-core Linux")
    ap.add_argument("--probe", choices=("a", "b", "both"), default="both",
                    help="which probe(s) to run (default both)")
    ap.add_argument("--best-mi", type=int, default=None,
                    help="override the Probe B max_inflight (else auto-selected from "
                         "Probe A, or a parameterized fallback if Probe B runs alone)")
    ap.add_argument("--no-mi32", action="store_true",
                    help="skip the optional guarded high-max_inflight cell in Probe A")
    args = ap.parse_args()
    return _run_and_print("full" if args.full else "smoke",
                          args.probe, args.best_mi, not args.no_mi32)


if __name__ == "__main__":
    sys.exit(main())
