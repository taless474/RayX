#!/usr/bin/env python3
"""Local validation aggregator for RayX smoke / contract checks.

One entry point that runs the existing quick smoke/contract checks in a
predictable order and prints a PASS / FAIL / SKIP line per check. It is NOT a
benchmark: no matrices, no timing/performance assertions -- only shape/contract
and golden-sequence checks. Each check runs in its own subprocess so one
failure does not mask the rest, and failures are collected (not fail-fast) then
summarized at the end.

Tiers (an unavailable component is SKIPped with a reason, never failed):

  * Always (pure stdlib, no Ray / no _rayx):
      - bench/smoke_service_sequence.py
  * If the `_rayx` extension imports:
      - bench/smoke_rayx.py
      - examples/rayx_basic.py
      - tiny one_by_one driver smoke + analyzer
      - tiny batch_wait driver smoke + analyzer
  * If the native hpx_synthetic_baseline binary exists:
      - bench/smoke_diag.py
      - native golden service-sequence JSONL check
  * If Ray is installed:
      - tiny Ray driver golden-sequence check

Scratch driver/native outputs go under results/_smoke_local/ (results/ is
gitignored) and are removed on exit.

Usage:
    python bench/smoke_local.py
Exit code 0 == every AVAILABLE component passed; nonzero == a real failure of an
available component.
"""
import json
import os
import shutil
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
NATIVE_BIN = os.path.join(REPO_ROOT, "hpx_impl", "build", "hpx_synthetic_baseline")
SCRATCH = os.path.join(REPO_ROOT, "results", "_smoke_local")

# Golden bimodal sequence for service-low=1, high=20, p-high=0.1, seed=0 -- the
# same anchors bench/smoke_service_sequence.py and the native CI golden check
# pin, so every engine must emit it identically.
GOLDEN = [20, 1, 1, 20, 1, 1, 1, 1]
GOLDEN_ARGS = ["--service-pattern", "bimodal", "--service-low", "1",
               "--service-high", "20", "--service-p-high", "0.1",
               "--seed", "0", "--requests", "8"]

_results = []  # list of (status, name); status in {"PASS", "FAIL", "SKIP"}


def _record(status, name, detail=""):
    line = f"{status}: {name}"
    if detail:
        line += f" ({detail})"
    print(line, flush=True)
    _results.append((status, name))


def _run(cmd):
    """Run cmd from the repo root with output captured."""
    return subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)


def _stderr_tail(proc, n=3):
    lines = [ln for ln in proc.stderr.splitlines() if ln.strip()]
    return " | ".join(lines[-n:]) if lines else f"exit {proc.returncode}"


def _script(name, rel_path, *args):
    """PASS/FAIL a python smoke script by its exit code."""
    proc = _run([PY, rel_path, *args])
    if proc.returncode == 0:
        _record("PASS", name)
    else:
        _record("FAIL", name, _stderr_tail(proc))


def _importable(module):
    """True if `import <module>` succeeds in a fresh subprocess.

    For rayx this prepends python/src so the repo-local package is found without
    requiring PYTHONPATH; we never import rayx into THIS process (so the pure
    stdlib checks always run even when the extension is missing).
    """
    if module == "rayx":
        code = f"import sys; sys.path.insert(0, {RAYX_SRC!r}); import rayx"
    else:
        code = f"import {module}"
    return _run([PY, "-c", code]).returncode == 0


def _driver_and_analyzer(name, retire_mode, extra, api="engine"):
    """Tiny rayx driver run (gitignored scratch) + analyzer rollup.

    Contract-only: confirms the driver runs end to end, the analyzer parses the
    JSONL, and the summary keeps schema_version "1". No timing assertions.
    `api` selects the driver API (engine/batch/...); for --api batch the
    retire_mode is inert (the driver records retire_mode="batch") but still names
    the scratch file uniquely.
    """
    out = os.path.join(SCRATCH, f"{retire_mode}_{api}.jsonl")
    drv = _run([PY, "bench/run_hpx_python_baseline.py", "--api", api,
                "--retire-mode", retire_mode, "--requests", "20",
                "--concurrency", "4", "--num-lanes", "2", "--hpx-threads", "4",
                "--service-ms", "0", "--out", out, *extra])
    if drv.returncode != 0:
        _record("FAIL", name, "driver: " + _stderr_tail(drv))
        return
    an = _run([PY, "bench/analyze_jsonl.py", out])
    if an.returncode != 0:
        _record("FAIL", name, "analyzer: " + _stderr_tail(an))
        return
    try:
        summary = json.loads(an.stdout)
    except json.JSONDecodeError as exc:
        _record("FAIL", name, f"analyzer output not JSON: {exc}")
        return
    if summary.get("schema_version") != "1":
        _record("FAIL", name, f"schema_version={summary.get('schema_version')!r}")
        return
    _record("PASS", name, f"schema=1 completed={summary.get('completed')}")


def _chunk_fields_ok(name, path, exp_chunks, exp_delay):
    """Assert every row's chunks/chunk_delay_ms match the requested values.

    Numeric compare (drivers may emit 4 vs 4.0): casts before comparing.
    """
    try:
        rows = [json.loads(ln) for ln in open(path) if ln.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        _record("FAIL", name, f"read/parse: {exc}")
        return False
    for r in rows:
        if int(r.get("chunks", -1)) != exp_chunks or \
                float(r.get("chunk_delay_ms", -1)) != float(exp_delay):
            _record("FAIL", name, f"row chunks/delay "
                    f"({r.get('chunks')},{r.get('chunk_delay_ms')}) != "
                    f"({exp_chunks},{exp_delay})")
            return False
    return True


def _chunked_driver(name, cmd, out, exp_chunks, exp_delay, analyze=False):
    """Run a driver chunked, assert rows carry chunks/delay; optionally analyze."""
    proc = _run(cmd)
    if proc.returncode != 0:
        _record("FAIL", name, "driver: " + _stderr_tail(proc))
        return
    if not _chunk_fields_ok(name, out, exp_chunks, exp_delay):
        return
    detail = f"chunks={exp_chunks} delay={exp_delay}"
    if analyze:
        an = _run([PY, "bench/analyze_jsonl.py", out])
        if an.returncode != 0:
            _record("FAIL", name, "analyzer: " + _stderr_tail(an))
            return
        try:
            if json.loads(an.stdout).get("schema_version") != "1":
                _record("FAIL", name, "analyzer schema_version != '1'")
                return
        except json.JSONDecodeError as exc:
            _record("FAIL", name, f"analyzer output not JSON: {exc}")
            return
        detail += "; analyzer schema=1"
    _record("PASS", name, detail)


def _rayx_chunked():
    """rayx driver: default emits 1/0, chunked emits requested, batch rejects."""
    name = "rayx chunked driver (default 1/0; chunked rows; batch rejects)"
    base = [PY, "bench/run_hpx_python_baseline.py", "--api", "engine",
            "--requests", "6", "--concurrency", "2", "--num-lanes", "1",
            "--hpx-threads", "2"]
    out_def = os.path.join(SCRATCH, "rayx_chunk_default.jsonl")
    d0 = _run(base + ["--service-ms", "2", "--out", out_def])
    if d0.returncode != 0:
        _record("FAIL", name, "default driver: " + _stderr_tail(d0))
        return
    if not _chunk_fields_ok(name, out_def, 1, 0):
        return
    out_chunk = os.path.join(SCRATCH, "rayx_chunk.jsonl")
    d1 = _run(base + ["--service-ms", "6", "--chunks", "3", "--chunk-delay-ms",
                      "1", "--work-mode", "spin", "--out", out_chunk])
    if d1.returncode != 0:
        _record("FAIL", name, "chunked driver: " + _stderr_tail(d1))
        return
    if not _chunk_fields_ok(name, out_chunk, 3, 1):
        return
    # Batch path must reject chunking (matches the facade: submit_batch unchunked).
    rej = _run([PY, "bench/run_hpx_python_baseline.py", "--api", "batch",
                "--requests", "2", "--service-ms", "5", "--chunks", "2",
                "--num-lanes", "1", "--hpx-threads", "2",
                "--out", os.path.join(SCRATCH, "rayx_rej.jsonl")])
    if rej.returncode == 0:
        _record("FAIL", name, "--api batch did not reject --chunks")
        return
    # Analyzer summarizes chunked output unchanged.
    an = _run([PY, "bench/analyze_jsonl.py", out_chunk])
    if an.returncode != 0 or json.loads(an.stdout or "{}").get("schema_version") != "1":
        _record("FAIL", name, "analyzer: " + _stderr_tail(an))
        return
    _record("PASS", name, "default 1/0; chunked 3/1; batch rejects; analyzer schema=1")


def _check_golden_file(name, path):
    try:
        with open(path) as f:
            rows = sorted((json.loads(ln) for ln in f if ln.strip()),
                          key=lambda r: r["request_id"])
    except (OSError, json.JSONDecodeError, KeyError) as exc:
        _record("FAIL", name, f"read/parse: {exc}")
        return
    seq = [r.get("service_ms_requested") for r in rows]
    if seq == GOLDEN:
        _record("PASS", name, f"seq={seq}")
    else:
        _record("FAIL", name, f"seq={seq} != {GOLDEN}")


def _native_golden():
    out = os.path.join(SCRATCH, "native_golden.jsonl")
    proc = _run([NATIVE_BIN, *GOLDEN_ARGS, "--work-mode", "sleep",
                 "--num-lanes", "1", "--out", out])
    if proc.returncode != 0:
        _record("FAIL", "native golden JSONL", _stderr_tail(proc))
        return
    _check_golden_file("native golden JSONL", out)


def _ray_golden():
    out = os.path.join(SCRATCH, "ray_golden.jsonl")
    proc = _run([PY, "bench/run_ray_baseline.py", *GOLDEN_ARGS,
                 "--concurrency", "1", "--out", out])
    if proc.returncode != 0:
        _record("FAIL", "Ray driver golden sequence", _stderr_tail(proc))
        return
    _check_golden_file("Ray driver golden sequence", out)


def main():
    os.makedirs(SCRATCH, exist_ok=True)
    try:
        # Tier 1: always (pure stdlib; no Ray, no _rayx).
        _script("service-sequence golden (smoke_service_sequence.py)",
                "bench/smoke_service_sequence.py")

        # Tier 2: RayX Python frontend (needs the built _rayx extension).
        if _importable("rayx"):
            _script("rayx contract smoke (smoke_rayx.py)", "bench/smoke_rayx.py")
            _script("API example (examples/rayx_basic.py)",
                    "examples/rayx_basic.py")
            _driver_and_analyzer("rayx one_by_one driver + analyzer",
                                 "one_by_one", [])
            _driver_and_analyzer("rayx batch_wait driver + analyzer",
                                 "batch_wait", ["--wait-batch", "4"])
            # Bulk-varied batch: bimodal sequence routed through the true
            # submit_batch(service_ms=[...]) crossing (low/high > 0 for batch).
            _driver_and_analyzer(
                "rayx batch bimodal (varied) driver + analyzer", "one_by_one",
                ["--service-pattern", "bimodal", "--service-low", "1",
                 "--service-high", "20", "--service-p-high", "0.2", "--seed", "1"],
                api="batch")
            _rayx_chunked()
        else:
            for skipped in ("rayx contract smoke (smoke_rayx.py)",
                            "API example (examples/rayx_basic.py)",
                            "rayx one_by_one driver + analyzer",
                            "rayx batch_wait driver + analyzer",
                            "rayx batch bimodal (varied) driver + analyzer",
                            "rayx chunked driver (default 1/0; chunked rows; "
                            "batch rejects)"):
                _record("SKIP", skipped,
                        "_rayx extension not importable (build it first)")

        # Tier 3: native HPX baseline binary.
        if os.path.exists(NATIVE_BIN):
            _script("native --diag smoke (smoke_diag.py)", "bench/smoke_diag.py")
            _native_golden()
            out = os.path.join(SCRATCH, "native_chunk.jsonl")
            _chunked_driver(
                "native chunked JSONL (chunks/delay rows)",
                [NATIVE_BIN, "--service-ms", "3", "--chunks", "3",
                 "--chunk-delay-ms", "1", "--work-mode", "sleep", "--num-lanes",
                 "1", "--requests", "6", "--out", out], out, 3, 1)
        else:
            _record("SKIP", "native --diag smoke (smoke_diag.py)",
                    "native binary not built")
            _record("SKIP", "native golden JSONL", "native binary not built")
            _record("SKIP", "native chunked JSONL (chunks/delay rows)",
                    "native binary not built")

        # Tier 4: Ray (optional).
        if _importable("ray"):
            _ray_golden()
            out = os.path.join(SCRATCH, "ray_chunk.jsonl")
            _chunked_driver(
                "Ray chunked JSONL (chunks/delay rows + analyzer)",
                [PY, "bench/run_ray_baseline.py", "--service-ms", "3",
                 "--chunks", "3", "--chunk-delay-ms", "1", "--requests", "6",
                 "--concurrency", "1", "--num-cpus", "2", "--out", out],
                out, 3, 1, analyze=True)
        else:
            _record("SKIP", "Ray driver golden sequence", "ray not installed")
            _record("SKIP", "Ray chunked JSONL (chunks/delay rows + analyzer)",
                    "ray not installed")
    finally:
        shutil.rmtree(SCRATCH, ignore_errors=True)

    passed = sum(1 for s, _ in _results if s == "PASS")
    failed = sum(1 for s, _ in _results if s == "FAIL")
    skipped = sum(1 for s, _ in _results if s == "SKIP")
    print(f"\nsmoke_local: {passed} passed, {failed} failed, {skipped} skipped")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
