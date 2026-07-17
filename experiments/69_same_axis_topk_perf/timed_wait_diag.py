#!/usr/bin/env python3
"""exp69 -- SEPARATE, NON-GATING Linux timed-wait diagnostic (NOT exp69 performance evidence).

Purpose: determine whether the sustained HPX timed-wait crash observed locally on macOS (a
connect-mode host process that dies -- SIGILL/SIGSEGV -- after a few hundred sequential
`hpx::future::wait_for` calls on a `run_as_hpx_thread` trampoline) reproduces on Linux, and confirm
the hardened path survives the same volume. It compares EXACTLY the two HPX wait strategies already
built into `exp69_actor_ext.coordinate_diag` (no new mechanism, no reinterpretation of exp64):

  * run_waitfor -- OLD: run_as_hpx_thread + hpx::future::wait_for (the exp68-style timed wait)
  * post_get    -- HARDENED (exp69's measured path): hpx::post + UNTIMED native merged.get() on the
                   HPX thread + a caller-side std::future::wait_for OS-thread bound (GIL released)

Topology (single node; loopback parcelport -- this is a wait-mechanism test, not a cross-node test):
a work-free exp69_peer root + one idle connect-mode worker B (serves the peer shard) + a fresh
connect-mode DIAG worker A per mode that loops coordinate_diag(mode, peer_loc=B, ...) `--iters` times.
This is fully INDEPENDENT of the measured Ray actors (plain subprocesses; no Ray anywhere).

Records per mode: return code / signal, iterations completed, first failing iteration (if any),
driver-sampled peak RSS and worker-reported ru_maxrss, plus platform/compiler/HPX identity. The
measured exp69 path is NOT altered by whatever this diagnostic finds; the hardened path stays.

  Build first (see exp69_crossnode.sbatch / CMakeLists.txt), then, e.g.:
    python experiments/69_same_axis_topk_perf/timed_wait_diag.py \
      --build-dir experiments/69_same_axis_topk_perf/build_rostam --iters 800
"""

import argparse
import importlib
import json
import os
import platform
import signal
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
# Module-level constants/helpers only (run_exp69 imports Ray lazily, never at module load).
from run_exp69 import (EXPECTED_HPX_COMMIT, EXT_MODULE, PEER_BASENAME, actor_endpoints,  # noqa: E402
                       commit_matches, find_free_port, peer_root_cmd)

DEFAULT_MODES = ["run_waitfor", "post_get"]  # OLD vs HARDENED -- the only two variants compared
# A P3b-shaped shard split: nonzero per-iteration work, small enough to loop hundreds of times fast.
DIAG_V, DIAG_SPLIT, DIAG_SEED, DIAG_K = 100_000, 50_000, 1, 100


# ---------------------------------------------------------------------------------------
# Small process/file helpers (bounded; local-loopback only)
# ---------------------------------------------------------------------------------------

def _popen(cmd, log_path):
    log = open(log_path, "w")
    return subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                            start_new_session=True), log


def _kill_group(proc):
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def _wait_for_file(path, timeout, procs=()):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return True
        for p in procs:
            if p is not None and p.poll() is not None:
                time.sleep(0.2)
                return os.path.exists(path)
        time.sleep(0.05)
    return os.path.exists(path)


def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _first_line(cmd):
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        return (out.stdout or out.stderr or "").strip().splitlines()[0] if (out.stdout or out.stderr) else None
    except Exception:  # noqa: BLE001
        return None


def _rss_kb(pid):
    """Best-effort resident set size in KB for `pid` (Linux /proc VmRSS, else ps)."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    try:
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True,
                             text=True, timeout=10)
        v = out.stdout.strip()
        return int(v) if v.isdigit() else None
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------------------
# Worker roles (plain subprocess; import the ext, join the root, do bounded work). No Ray.
# ---------------------------------------------------------------------------------------

def _load_ext(build_dir):
    sys.path.insert(0, build_dir)
    return importlib.import_module(EXT_MODULE)


def run_idle_worker(args):
    """Idle connect-mode worker B: joins, publishes its locality id, serves the peer shard until
    the stop sentinel appears."""
    e = _load_ext(args.build_dir)
    endpoints = args.endpoints.split(",") if args.endpoints else []
    e.start_connect(args.hpx_threads, endpoints)
    loc = int(e.locality_id())
    with open(args.marker + ".tmp", "w") as f:
        json.dump({"locality_id": loc, "pid": os.getpid(), "hostname": socket.gethostname()}, f)
    os.replace(args.marker + ".tmp", args.marker)
    deadline = time.time() + args.max_seconds
    while time.time() < deadline and not os.path.exists(args.stop):
        time.sleep(0.1)
    try:
        e.stop_disconnect()
    except Exception:  # noqa: BLE001
        pass
    return 0


def run_diag_worker(args):
    """Diag worker A: loop coordinate_diag(mode, peer_loc=B, ...) `--iters` times, recording the
    last-completed iteration each step so a crash reveals the first failing iteration."""
    import resource
    e = _load_ext(args.build_dir)
    endpoints = args.endpoints.split(",") if args.endpoints else []
    e.start_connect(args.hpx_threads, endpoints)
    own_lo, own_hi = 0, DIAG_SPLIT
    peer_lo, peer_hi = DIAG_SPLIT, DIAG_V
    last_ready = last_found = None
    completed = 0
    for i in range(1, args.iters + 1):
        d = e.coordinate_diag(args.mode, args.peer_loc, own_lo, own_hi, peer_lo, peer_hi,
                              DIAG_SEED, DIAG_K, args.bound_s)
        last_ready, last_found = bool(d.get("ready")), bool(d.get("target_found"))
        completed = i
        with open(args.progress + ".tmp", "w") as f:
            f.write(str(i))
        os.replace(args.progress + ".tmp", args.progress)
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # Linux: KB; macOS: bytes
    with open(args.done + ".tmp", "w") as f:
        json.dump({"completed": completed, "iters_requested": args.iters, "mode": args.mode,
                   "last_ready": last_ready, "last_found": last_found, "ru_maxrss": maxrss,
                   "pid": os.getpid(), "hostname": socket.gethostname()}, f)
    os.replace(args.done + ".tmp", args.done)
    try:
        e.stop_disconnect()
    except Exception:  # noqa: BLE001
        pass
    return 0


# ---------------------------------------------------------------------------------------
# Driver: stand up the island, run each mode in a fresh worker, monitor crash/RSS, report.
# ---------------------------------------------------------------------------------------

def _run_mode(mode, build_dir, p_root, boot, peer_loc, iters, bound_s, hpx_threads):
    p_a = find_free_port()
    ep_a = actor_endpoints(p_root, p_a)  # loopback endpoints (localhost:port)
    progress = os.path.join(boot, f"diag_{mode}.progress")
    done = os.path.join(boot, f"diag_{mode}.done")
    for p in (progress, done):
        if os.path.exists(p):
            os.remove(p)
    # NOTE: HPX endpoint tokens start with "--hpx:", so they MUST be passed via --endpoints=<value>
    # (argparse would otherwise treat the leading "--" as a new option and reject the value).
    cmd = [sys.executable, os.path.abspath(__file__), "--role", "diag", "--build-dir", build_dir,
           f"--endpoints={','.join(ep_a)}", "--hpx-threads", str(hpx_threads), "--mode", mode,
           "--iters", str(iters), "--peer-loc", str(peer_loc), "--bound-s", str(bound_s),
           "--progress", progress, "--done", done]
    proc, log = _popen(cmd, os.path.join(boot, f"diag_{mode}.log"))
    peak_rss_kb = None
    overall_deadline = time.time() + iters * bound_s + 300
    try:
        while proc.poll() is None and time.time() < overall_deadline:
            rss = _rss_kb(proc.pid)
            if rss is not None:
                peak_rss_kb = rss if peak_rss_kb is None else max(peak_rss_kb, rss)
            time.sleep(0.25)
        if proc.poll() is None:
            _kill_group(proc)
            proc.wait(timeout=10)
    finally:
        log.close()
    rc = proc.returncode
    sig = -rc if (rc is not None and rc < 0) else None
    sig_name = signal.Signals(sig).name if sig else None
    last_progress = None
    if os.path.exists(progress):
        try:
            last_progress = int(open(progress).read().strip() or "0")
        except ValueError:
            last_progress = None
    done_blob = _read_json(done)
    completed_clean = bool(done_blob) and done_blob.get("completed") == iters
    crashed = sig is not None or (rc not in (0, None) and not completed_clean)
    first_failing = None
    if crashed:
        first_failing = (last_progress or 0) + 1
    return {
        "mode": mode, "iters_requested": iters, "return_code": rc, "signal": sig,
        "signal_name": sig_name, "completed_iterations": (done_blob or {}).get("completed",
                                                                              last_progress),
        "completed_clean": completed_clean, "crashed": crashed,
        "first_failing_iteration": first_failing,
        "driver_peak_rss_kb": peak_rss_kb, "worker_ru_maxrss": (done_blob or {}).get("ru_maxrss"),
        "last_result_ready": (done_blob or {}).get("last_ready"),
        "last_result_found": (done_blob or {}).get("last_found"),
        "endpoints": ep_a,
    }


def run_driver(args):
    peer = os.path.join(args.build_dir, PEER_BASENAME)
    ext_so = next((fn for fn in (os.listdir(args.build_dir) if os.path.isdir(args.build_dir) else [])
                   if fn.startswith(EXT_MODULE) and fn.endswith(".so")), None)
    if not (os.path.exists(peer) and ext_so):
        print(f"SKIP: build exp69 first (peer={os.path.exists(peer)} ext={bool(ext_so)})")
        return 0

    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    ext = _load_ext(args.build_dir)
    hpx_info = dict(ext.hpx_version_info())
    hpx_cv = hpx_info.get("hpx_complete_version")
    result = {
        "experiment": "69_same_axis_topk_perf",
        "artifact": "linux_timed_wait_diagnostic",
        "non_gating": True, "not_exp69_performance_evidence": True,
        "compares": {"run_waitfor": "OLD: run_as_hpx_thread + hpx::future::wait_for",
                     "post_get": "HARDENED (measured path): hpx::post + untimed get() + "
                                 "caller std::future::wait_for"},
        "does_not_alter_measured_path": True,
        "does_not_reinterpret_exp64_waiter_fix": True,
        "platform": platform.platform(), "machine": platform.machine(),
        "system": platform.system(), "python_version": sys.version.split()[0],
        "python_compiler": platform.python_compiler(),
        "cxx_version": _first_line(["c++", "--version"]) or _first_line(["g++", "--version"]),
        "gcc_version": _first_line(["gcc", "--version"]),
        "hpx_complete_version": hpx_cv,
        "hpx_commit_matches_fixed": bool(commit_matches(hpx_cv)),
        "expected_hpx_commit": EXPECTED_HPX_COMMIT,
        "iters": args.iters, "bound_s": args.bound_s, "modes": modes,
        "topology": "work-free exp69_peer root + idle worker B (peer shard) + fresh diag worker A "
                    "per mode; single node, loopback parcelport; NO Ray",
    }

    boot = args.out_dir or os.path.join(HERE, "_exp69_runs",
                                        f"timed_wait_diag_{time.strftime('%Y%m%dT%H%M%SZ')}")
    os.makedirs(boot, exist_ok=True)
    p_root, p_b = find_free_port(), find_free_port()
    root_cmd = peer_root_cmd(peer, boot, p_root)
    ep_b = actor_endpoints(p_root, p_b)
    b_marker = os.path.join(boot, "worker_b.locality")
    b_stop = os.path.join(boot, "worker_b.stop")
    root = rootlog = worker_b = wblog = None
    modes_out = []
    try:
        root, rootlog = _popen(root_cmd, os.path.join(boot, "root.log"))
        if not _wait_for_file(os.path.join(boot, "root.ready"), 30, procs=[root]):
            result["overall"] = "error"; result["reason"] = "root did not become ready"
            return _finish(result, modes_out, boot, args)
        wb_cmd = [sys.executable, os.path.abspath(__file__), "--role", "idle",
                  "--build-dir", os.path.abspath(args.build_dir), f"--endpoints={','.join(ep_b)}",
                  "--hpx-threads", str(args.hpx_threads), "--marker", b_marker, "--stop", b_stop,
                  "--max-seconds", str(args.iters * args.bound_s + 600)]
        worker_b, wblog = _popen(wb_cmd, os.path.join(boot, "worker_b.log"))
        if not _wait_for_file(b_marker, 60, procs=[worker_b]):
            result["overall"] = "error"; result["reason"] = "idle worker B did not join"
            return _finish(result, modes_out, boot, args)
        b_loc = (_read_json(b_marker) or {}).get("locality_id")
        result["idle_worker_b_locality"] = b_loc
        for mode in modes:
            print(f"[timed-wait-diag] mode {mode}: {args.iters} iters ...", flush=True)
            mr = _run_mode(mode, os.path.abspath(args.build_dir), p_root, boot, b_loc,
                           args.iters, args.bound_s, args.hpx_threads)
            print(f"[timed-wait-diag]   {mode}: rc={mr['return_code']} signal={mr['signal_name']} "
                  f"completed={mr['completed_iterations']}/{args.iters} crashed={mr['crashed']} "
                  f"first_fail={mr['first_failing_iteration']}", flush=True)
            modes_out.append(mr)
    finally:
        open(b_stop, "w").close()
        if worker_b is not None:
            try:
                worker_b.wait(timeout=30)
            except Exception:  # noqa: BLE001
                _kill_group(worker_b)
        open(os.path.join(boot, "root.done"), "w").close()
        _wait_for_file(os.path.join(boot, "root.final"), 40, procs=[root])
        _kill_group(root)
        for lf in (rootlog, wblog):
            if lf is not None:
                lf.close()
    return _finish(result, modes_out, boot, args)


def _finish(result, modes_out, boot, args):
    result["modes_result"] = modes_out
    by = {m["mode"]: m for m in modes_out}
    old = by.get("run_waitfor")
    hard = by.get("post_get")
    result["old_variant_crashed"] = (bool(old["crashed"]) if old else None)
    result["hardened_variant_completed"] = (bool(hard["completed_clean"]) if hard else None)
    result["overall"] = result.get("overall") or "complete"
    result["summary"] = (
        f"old(run_waitfor) crashed={result['old_variant_crashed']} "
        f"first_fail={old['first_failing_iteration'] if old else None}; "
        f"hardened(post_get) completed_clean={result['hardened_variant_completed']} "
        f"({(hard or {}).get('completed_iterations')}/{args.iters})")
    out = args.out or os.path.join(boot, "timed_wait_diag.json")
    with open(out, "w") as f:
        json.dump(result, f, indent=2); f.write("\n")
    print(f"[timed-wait-diag] {result['summary']}")
    print(f"[timed-wait-diag] -> {out}")
    return 0


def main():
    ap = argparse.ArgumentParser(description="exp69 non-gating Linux timed-wait diagnostic")
    ap.add_argument("--role", choices=["driver", "idle", "diag"], default="driver")
    ap.add_argument("--build-dir", default=os.path.join(HERE, "build"))
    ap.add_argument("--iters", type=int, default=800, help="bounded iterations per mode")
    ap.add_argument("--bound-s", type=int, default=30, help="per-call caller/HPX wait bound (s)")
    ap.add_argument("--hpx-threads", type=int, default=2)
    ap.add_argument("--modes", default=",".join(DEFAULT_MODES))
    ap.add_argument("--out", default=None, help="driver: aggregate JSON path")
    ap.add_argument("--out-dir", default=None, help="driver: run/boot directory")
    # worker-only args
    ap.add_argument("--endpoints", default="")
    ap.add_argument("--marker", default=None)
    ap.add_argument("--stop", default=None)
    ap.add_argument("--max-seconds", type=int, default=3600)
    ap.add_argument("--mode", default=None)
    ap.add_argument("--peer-loc", type=int, default=None)
    ap.add_argument("--progress", default=None)
    ap.add_argument("--done", default=None)
    args = ap.parse_args()
    if args.role == "idle":
        return run_idle_worker(args)
    if args.role == "diag":
        return run_diag_worker(args)
    return run_driver(args)


if __name__ == "__main__":
    sys.exit(main())
