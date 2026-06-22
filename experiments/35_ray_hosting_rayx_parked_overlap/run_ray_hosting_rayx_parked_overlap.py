#!/usr/bin/env python3
"""exp35 -- Ray-hosted RayX cooperative-suspension preservation under parked load.

ADAPTER NON-REGRESSION PROBE (observation-only, machine-specific). HPX cooperative
park-vs-compute suspension is TRUE BY CONSTRUCTION for `hpx::this_thread::sleep_for`
(a suspended HPX task vacates its OS worker). exp35 does NOT claim to discover
overlap. It tests whether the Ray-hosted RayX stack -- one `Runtime` over FIFO
`RuntimeLane`s, `DispatchPolicy=Async`, driven by a CPython closed loop inside one
Ray actor -- PRESERVES that HPX property at fixed W, and how much shared-FIFO
admission and the CPython driver erode it.

QUESTION:
  Does the Ray-hosted RayX Runtime / FIFO RuntimeLane / DispatchPolicy=Async /
  CPython closed-loop stack preserve HPX cooperative park-vs-compute suspension at
  fixed W, and how much do (a) shared-FIFO admission and (b) the CPython driver
  erode it?

THREE ARMS (per cell):
  * Arm C  -- compute-only:  K_C  busy_sum(n_c) ops.
  * Arm Wp -- parked-only:   K_Wp park_ms(ms) ops (park-vs-park reference).
  * Arm T  -- matched:       same K_C compute PLUS K_Wp parked, class-aware closed
               loop holding compute concurrency at O_C and adding parked on top.

PRIMARY DISCRIMINATOR -- C-class RETENTION: cooperative parking (park_ms suspends
the HPX task, frees the worker) keeps compute throughput/latency intact when parks
are added. Worker-pinning / park-vs-park-only behavior would degrade compute.

PARKED-DEMAND CALIBRATION (so the wall-based indicator is valid): per cell, run
Arm C first, measure wall_C, then set the 1x K_Wp so parked-only wall_Wp is roughly
comparable to wall_C; the max-vs-sum indicator is computed ONLY when
wall_Wp/wall_C in [1/3, 3], else reported NA. Coarse cells are retention-only when
walls are not comparable -- coarse is NOT a max-vs-sum negative control.

CONFOUNDS NAMED (must stay visible):
  * Shared FIFO lanes: RuntimeLane is FIFO per lane and Python cannot pin a class to
    a lane without an API change, so a compute op can queue behind a parked op
    before dispatch. C retention is therefore an ADAPTER-level result -- cooperative
    suspension PLUS FIFO admission effects -- not proof of pure worker-pool overlap.
    (park_ms is Async, so the lane consumer dispatches it non-blocking via
    hpx::async; the admission coupling is dispatch-latency-scale, not park_ms-scale,
    but it is not zero and grows with parked density.)
  * CPython driver: the closed loop submits/retires under the GIL; if Python cannot
    keep HPX backlogged, wall-based indicators measure the DRIVER, not the
    scheduler. The in-scope control is the racy `lane_stats()` backlog/active trace:
    a starved trace forces INCONCLUSIVE for the wall indicators.

GATES (the ONLY pass/fail): agg_ok, futures_completed, plain_types_ok, lane_ids_ok,
clean_shutdown. Timing / retention / max-vs-sum / lane_stats are READING criteria
only, NEVER structural gates. `lane_stats()` snapshots are racy/approximate,
context-only. Exit 0 = gates passed or cleanly skipped (Ray or rayx.runtime
unavailable); exit 1 = a structural gate failed.

NOT real I/O, NOT inference, NOT Ray Serve, NOT Ray cluster scaling, NOT HPX
priority scheduling, NOT a latency-SLO/capacity/sizing claim, NOT "RayX makes Ray
faster" / "HPX beats Ray" / "RayX replaces Ray", and NO socket/NUMA attribution.
`lane_impl="std"` only; no W=32; no NUMA/binding; no priorities/pools/counters.

Usage:
    python experiments/35_ray_hosting_rayx_parked_overlap/run_ray_hosting_rayx_parked_overlap.py            # --smoke (default)
    python experiments/35_ray_hosting_rayx_parked_overlap/run_ray_hosting_rayx_parked_overlap.py --full
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

# max-vs-sum is valid only when wall_C and wall_Wp are comparable.
COMPARABLE_LO, COMPARABLE_HI = 1.0 / 3.0, 3.0

# Cap on K_Wp from calibration, so timer/rescheduling overhead of an absurd parked
# count cannot masquerade as "no overlap" (coarse cells will hit this and become
# retention-only -- by design, NOT a negative control).
K_WP_CAP = 400

# Parked-density sweep (multiples of the calibrated 1x K_Wp).
DENSITIES_FULL = (0.5, 1.0, 2.0)
DENSITIES_SMOKE = (1.0,)

# Reading thresholds (READING criteria only; NEVER structural gates).
RET_THR_SUPPORT = 0.90    # C throughput retention >= this = compute intact
RET_P90_SUPPORT = 1.20    # C p90 retention <= this = latency intact
RET_THR_STOP = 0.70       # C throughput retention <= this = degraded
MVS_SUPPORT = 0.70        # max-vs-sum position >= this (where comparable) corroborates
MVS_STOP = 0.30           # max-vs-sum position <= this = serialized

# ---- smoke-mode parameters (laptop-safe DEFAULT; SMOKE-ONLY, not evidence) ----
W_SMOKE = (4,)
KC_SMOKE = 16
REPS_SMOKE = 2
WARMUP_SMOKE = 1
LEVELS_SMOKE = ("over",)
GRAN_SMOKE = (("fine", 2_000_000),)

# ---- full-mode parameters (homogeneous many-core Linux; observation-only) ------
W_FULL = (4, 8, 16)       # NO W=32
KC_FULL = 60              # reps=3 -> pooled C >= 180 so p99 retention is eligible
REPS_FULL = 3
WARMUP_FULL = 1
LEVELS_FULL = ("near", "over")
# fine = headline (full density sweep, max-vs-sum where comparable);
# coarse = retention-only compute-bound check (NOT a max-vs-sum negative control).
GRAN_FULL = (("fine", 2_000_000), ("coarse", 20_000_000))

# How often (in retire iterations) to sample lane_stats during an arm.
LANE_SAMPLE_STRIDE = 3


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


def outstanding_for(level, w):
    """Closed-loop window relative to W (the actor clamps to the class count)."""
    if level == "under":
        return max(1, w // 2)
    if level == "near":
        return w
    return 4 * w  # over


# --------------------------------------------------------------------------- #
# Ray actor (one per (W, granularity); hosts ONE Runtime)                      #
# --------------------------------------------------------------------------- #
def _build_actor(ray):
    """Define the Ray actor inside a function so this module imports without Ray
    present (clean-skip path)."""

    @ray.remote
    class RayxParkedActor:
        """Hosts ONE rayx.runtime.Runtime(num_lanes=W, hpx_threads=W), lane_impl
        "std" by construction. Runs one arm (one or more request classes) over a
        CLASS-AWARE CLOSED LOOP, sampling the racy lane_stats() backlog/active trace.
        RuntimeFuture / OperationResult are created and retired INSIDE this actor;
        only plain scalars/lists/dicts cross the Ray boundary."""

        def __init__(self, w, rayx_src):
            import sys as _sys
            if rayx_src not in _sys.path:
                _sys.path.insert(0, rayx_src)
            from rayx.runtime import Runtime  # raises if _rayx missing
            self._rt = Runtime(num_lanes=w, hpx_threads=w)
            self._w = w

        def lane_ids(self):
            return [d["actor_id"] for d in self._rt.lane_stats()]

        def run_arm(self, classes, sample_stride):
            """Run one arm. `classes` maps class label -> {op_id, arg, count,
            window}. Holds each class's concurrency at its window, refilling as ops
            retire. Returns ONLY plain rows/aggregates.

            `wall_compute_ms` is the time until the last C (busy_sum) op retires
            (None if no C class); `wall_total_ms` is the full-arm wall. Per-class
            value_sum (plain int) lets the DRIVER check correctness against the
            closed form -- the actor does not import the driver module."""
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

    return RayxParkedActor


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


def _run_cells(w_values, gran_specs, levels, k_c, densities, reps, warmup):
    """Run every (W, granularity, level, density) and return (cells, gates_dict,
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
    max_w = max(w_values)
    ray.init(num_cpus=max_w + 1, ignore_reinit_error=True,
             log_to_driver=False, configure_logging=False)
    RayxParkedActor = _build_actor(ray)
    cells = []
    try:
        for w in w_values:
            for gran_name, n_c in gran_specs:
                actor = RayxParkedActor.options(num_cpus=w).remote(w, RAYX_SRC)
                ids = ray.get(actor.lane_ids.remote())
                if len(ids) != w or not all(
                        isinstance(i, str) and i.startswith("rt-hpx-") for i in ids):
                    gates.lane_ids_ok = False
                for level in levels:
                    o = outstanding_for(level, w)
                    # ---- Arm C (compute-only) -> wall_C, C baseline -------------
                    arm_c = _measure_arm(
                        ray, actor,
                        {"C": {"op_id": "busy_sum", "arg": n_c, "count": k_c,
                               "window": o}},
                        reps, warmup, gates)
                    wall_C = arm_c["wall_compute_med"]
                    # ---- calibrate 1x K_Wp so wall_Wp ~ wall_C -----------------
                    k_wp_1x = int(round(wall_C * o / PARK_MS)) if PARK_MS > 0 else o
                    k_wp_1x = max(w, min(K_WP_CAP, k_wp_1x))
                    for d in densities:
                        k_wp = max(1, int(round(d * k_wp_1x)))
                        # ---- Arm Wp (parked-only) -> wall_Wp -------------------
                        arm_wp = _measure_arm(
                            ray, actor,
                            {"Wp": {"op_id": "park_ms", "arg": PARK_MS,
                                    "count": k_wp, "window": o}},
                            reps, warmup, gates)
                        wall_Wp = arm_wp["wall_total_med"]
                        # ---- Arm T (matched) ----------------------------------
                        arm_t = _measure_arm(
                            ray, actor,
                            {"C": {"op_id": "busy_sum", "arg": n_c, "count": k_c,
                                   "window": o},
                             "Wp": {"op_id": "park_ms", "arg": PARK_MS,
                                    "count": k_wp, "window": o}},
                            reps, warmup, gates)
                        cells.append(_cell_metrics(
                            w, gran_name, n_c, level, o, k_c, k_wp, d, reps,
                            arm_c, arm_wp, arm_t))
                try:
                    ray.get(actor.shutdown.remote())
                except Exception:
                    gates.clean_shutdown = False
                ray.kill(actor)
    finally:
        ray.shutdown()
    return cells, gates.as_dict(), False, None


def _cell_metrics(w, gran, n_c, level, o, k_c, k_wp, density, reps,
                  arm_c, arm_wp, arm_t):
    """Reduce one (W, gran, level, density) cell to a plain reading dict."""
    wall_C = arm_c["wall_compute_med"]
    wall_Wp = arm_wp["wall_total_med"]
    wall_CWp = arm_t["wall_compute_med"]
    wall_CWp_total = arm_t["wall_total_med"]

    def thr(pooled, wall_ms, n):
        # throughput from the compute-completion wall (per-call basis).
        return (n / (wall_ms / 1e3)) if wall_ms > 0 else float("nan")

    thr_C_ref = thr(arm_c["pooled_C"], wall_C, k_c)
    thr_C_test = thr(arm_t["pooled_C"], wall_CWp, k_c)
    thr_retention = (thr_C_test / thr_C_ref) if thr_C_ref > 0 else float("nan")

    cC, cT = arm_c["pooled_C"], arm_t["pooled_C"]
    n_samples = min(len(cC), len(cT))

    def ret(q):
        a, b = _pct(cC, q), _pct(cT, q)
        return (b / a) if a > 0 else float("nan")

    p99_ret = ret(99) if n_samples >= P99_MIN_SAMPLES else None

    parked_demand = float(k_wp * PARK_MS)            # per-call demand
    added_wall = wall_CWp - wall_C
    added_wall_fraction = (added_wall / parked_demand) if parked_demand > 0 else float("nan")

    # max-vs-sum position, ONLY when wall_C and wall_Wp are comparable.
    comparable = (wall_C > 0 and COMPARABLE_LO <= (wall_Wp / wall_C) <= COMPARABLE_HI)
    if comparable:
        denom = (wall_C + wall_Wp) - max(wall_C, wall_Wp)
        mvs = (((wall_C + wall_Wp) - wall_CWp) / denom) if denom > 0 else float("nan")
    else:
        mvs = None

    return {
        "w": w, "gran": gran, "n_c": n_c, "level": level, "outstanding": o,
        "k_c": k_c, "k_wp": k_wp, "density": density, "reps": reps,
        "wall_C": wall_C, "wall_Wp": wall_Wp,
        "wall_CWp": wall_CWp, "wall_CWp_total": wall_CWp_total,
        "thr_retention": thr_retention,
        "p50_ret": ret(50), "p90_ret": ret(90), "p99_ret": p99_ret,
        "n_samples_C": n_samples,
        "parked_demand_ms": parked_demand,
        "added_wall": added_wall, "added_wall_fraction": added_wall_fraction,
        "comparable": comparable, "max_vs_sum": mvs,
        "lane_trace": arm_t["lane_trace"],
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
def _cap_w_values(w_values, cpu):
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


def _fmt(x, spec=".2f"):
    if x is None:
        return "NA"
    try:
        if x != x:  # NaN
            return "NA"
    except TypeError:
        return "NA"
    return format(x, spec)


def _print_cell_table(cells):
    last_key = None
    for c in sorted(cells, key=lambda x: (x["w"], x["gran"], x["level"], x["density"])):
        key = (c["w"], c["gran"], c["level"])
        if key != last_key:
            print(f"  W={c['w']}  granularity={c['gran']} (n_c={c['n_c']}, "
                  f"cpc={checkpoint_count(c['n_c'])})  load={c['level']} "
                  f"outstanding={c['outstanding']}  K_C={c['k_c']} reps={c['reps']}")
            last_key = key
        lt = c["lane_trace"]
        print(f"    density={c['density']:<4} K_Wp={c['k_wp']:<4} "
              f"wall_C={_fmt(c['wall_C'],'.1f')} wall_Wp={_fmt(c['wall_Wp'],'.1f')} "
              f"wall_CWp={_fmt(c['wall_CWp'],'.1f')} (total={_fmt(c['wall_CWp_total'],'.1f')}) ms")
        print(f"        thr_retention={_fmt(c['thr_retention'])}  "
              f"p50/p90/p99 ret={_fmt(c['p50_ret'])}/{_fmt(c['p90_ret'])}/"
              f"{_fmt(c['p99_ret'])} (Cn={c['n_samples_C']})  "
              f"added_wall={_fmt(c['added_wall'],'.1f')}ms "
              f"(/demand={_fmt(c['added_wall_fraction'])})")
        print(f"        max_vs_sum={_fmt(c['max_vs_sum'])}"
              f"{'' if c['comparable'] else ' (NA: walls not comparable)'}  "
              f"lane_stats[backlog={lt['backlog_seen']} "
              f"active_frac={_fmt(lt['active_fraction'])} "
              f"qd_mean={_fmt(lt['qd_mean'],'.1f')} qd_max={lt['qd_max']}]")
    print()


def _interpret(cells, mode):
    """Reading (observation-only). SUPPORT = the adapter PRESERVES HPX cooperative
    suspension (compute retained under added parked load, robust across density,
    HPX backlogged); STOP = it does not; INCONCLUSIVE = driver-starved / sparse /
    unreadable. Smoke is always INCONCLUSIVE (smoke-only)."""
    fine = [c for c in cells if c["gran"] == "fine"]
    # driver-governor control: was HPX backlogged/active during the matched arms?
    backlog_any = any(c["lane_trace"]["backlog_seen"] for c in cells)
    active_mean = (statistics.fmean([c["lane_trace"]["active_fraction"] for c in cells])
                   if cells else 0.0)
    any_p99 = any(c["p99_ret"] is not None for c in cells)

    if mode == "smoke":
        return ("INCONCLUSIVE (smoke-only)",
                "smoke validates the three-arm / calibration / lane_stats structure "
                "only -- sample counts are low (p99 retention NA expected) and this "
                "is NOT evidence. Run --full (3 independent runs) on a homogeneous "
                "many-core Linux node for an observation.")
    if not backlog_any and active_mean < 0.1:
        return ("INCONCLUSIVE",
                "lane_stats shows HPX was not backlogged/active during the matched "
                "arms (driver-governed): the CPython submit/retire loop may have "
                "starved HPX, so wall-based indicators measure the driver, not the "
                "scheduler. Re-run with larger K / windows.")
    if not any_p99:
        return ("INCONCLUSIVE",
                "no cell reached the C p99 retention sample floor; tail retention is "
                "unreadable this run (raise K_C or reps).")
    ref = fine or cells
    thr_rets = [c["thr_retention"] for c in ref if c["thr_retention"] == c["thr_retention"]]
    p90_rets = [c["p90_ret"] for c in ref if c["p90_ret"] == c["p90_ret"]]
    mvs_vals = [c["max_vs_sum"] for c in ref
                if c["comparable"] and c["max_vs_sum"] is not None
                and c["max_vs_sum"] == c["max_vs_sum"]]
    thr_ok = thr_rets and min(thr_rets) >= RET_THR_SUPPORT
    p90_ok = (not p90_rets) or max(p90_rets) <= RET_P90_SUPPORT
    thr_bad = thr_rets and min(thr_rets) <= RET_THR_STOP
    mvs_ok = (not mvs_vals) or min(mvs_vals) >= MVS_SUPPORT
    mvs_bad = mvs_vals and min(mvs_vals) <= MVS_STOP

    if thr_ok and p90_ok and mvs_ok:
        return ("SUPPORT",
                "the Ray-hosted RayX adapter PRESERVES HPX cooperative "
                "park-vs-compute suspension at fixed W: compute throughput "
                f"retention >= {RET_THR_SUPPORT:g} and p90 retention <= "
                f"{RET_P90_SUPPORT:g} under added synthetic parked load, robust "
                "across the density sweep, DESPITE shared FIFO lanes; lane_stats "
                "confirms HPX stayed backlogged/active (not a starved-driver "
                "artifact). This is adapter PRESERVATION of an HPX property that is "
                "true by construction -- NOT a discovery, NOT pure worker-pool "
                "overlap (FIFO admission is folded in), NOT real I/O, NOT HPX "
                "priority scheduling, NOT 'HPX beats Ray' / 'RayX makes Ray faster'.")
    if thr_bad or mvs_bad:
        return ("STOP",
                "compute-class retention was eroded under added parked load "
                f"(throughput retention <= {RET_THR_STOP:g} and/or max-vs-sum <= "
                f"{MVS_STOP:g} where comparable): an adapter-level STOP, consistent "
                "with shared-FIFO admission / closed-loop load-shape sensitivity. It "
                "does NOT prove parked demand was fully serialized against compute -- "
                "especially where added_wall_fraction shows much of the parked demand "
                "was still overlapped. Worth recording -- something in the adapter "
                "path erodes HPX cooperative suspension at this load shape.")
    return ("INCONCLUSIVE",
            "retention sits between the SUPPORT and STOP bands and/or walls were not "
            "comparable enough to read max-vs-sum; cannot cleanly separate adapter "
            "preservation from FIFO-admission / driver noise this run.")


def _print_reading(cells, mode):
    verdict, msg = _interpret(cells, mode)
    print(f"READING (observation-only, this run/machine) [{verdict}]: {msg}")
    print("  HPX cooperative suspension is TRUE BY CONSTRUCTION for "
          "hpx::this_thread::sleep_for; exp35 tests ADAPTER PRESERVATION, not "
          "discovery of overlap.")
    print("  Shared FIFO lanes are a confound: a compute op can queue behind a "
          "parked op before dispatch, so C retention is an ADAPTER result "
          "(cooperative suspension + FIFO admission), not pure worker-pool overlap.")
    print("  lane_stats() snapshots are racy/approximate, context-only, never a "
          "gate; max-vs-sum is valid only where wall_C and wall_Wp are comparable.")
    print("  park_ms is SYNTHETIC cooperative parked wait (NOT real I/O, NOT "
          "inference). End-to-end / Ray boundary not a pure engine metric. Not Ray "
          "Serve, not cluster scaling, not HPX priority scheduling, not a "
          "latency-SLO/capacity claim.")
    print()


def _run_and_print(mode):
    if mode == "smoke":
        w_values, warn = _cap_w_values(W_SMOKE, os.cpu_count())
        gran_specs, levels = GRAN_SMOKE, LEVELS_SMOKE
        k_c, densities = KC_SMOKE, DENSITIES_SMOKE
        reps, warmup = REPS_SMOKE, WARMUP_SMOKE
        title = ("exp35 parked-overlap adapter non-regression (SMOKE) -- structural "
                 "validation only; SMOKE-ONLY, not evidence")
    else:
        w_values, warn = _cap_w_values(W_FULL, os.cpu_count())
        gran_specs, levels = GRAN_FULL, LEVELS_FULL
        k_c, densities = KC_FULL, DENSITIES_FULL
        reps, warmup = REPS_FULL, WARMUP_FULL
        title = ("exp35 parked-overlap adapter non-regression (FULL) -- homogeneous "
                 "many-core Linux; observation-only, machine-specific")

    print(title)
    _print_machine_info()
    print("NOTE -- ADAPTER NON-REGRESSION probe. HPX cooperative park-vs-compute")
    print("  suspension is TRUE BY CONSTRUCTION; exp35 tests whether the Ray-hosted")
    print("  RayX Runtime / FIFO RuntimeLane / Async-dispatch / CPython closed-loop")
    print("  stack PRESERVES it at fixed W. lane_impl=std only, NO W=32, NO NUMA/")
    print("  binding, NO priorities/pools/counters. park_ms is synthetic (not I/O).")
    if mode != "smoke" and not sys.platform.startswith("linux"):
        print("  On Mac/laptop/heterogeneous hardware this --full output is "
              "SMOKE-ONLY, not evidence.")
    if warn:
        print(f"  {warn}")
    print(f"  W_values={list(w_values)} granularities={[g[0] for g in gran_specs]} "
          f"levels={list(levels)} K_C={k_c} park_ms={PARK_MS} "
          f"densities={list(densities)} reps={reps} warmup={warmup} "
          f"p99_min_samples={P99_MIN_SAMPLES}")
    print()

    cells, gates, skipped, reason = _run_cells(
        w_values, gran_specs, levels, k_c, densities, reps, warmup)
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
                    help="structural smoke (the default): W=4, fine, small K_C, 1x "
                         "density; SMOKE-ONLY, not evidence")
    ap.add_argument("--full", action="store_true",
                    help="W in {4,8,16} x fine+coarse x near/over x density "
                         "{0.5,1,2} (homogeneous many-core Linux; no W=32)")
    args = ap.parse_args()
    return _run_and_print("full" if args.full else "smoke")


if __name__ == "__main__":
    sys.exit(main())
