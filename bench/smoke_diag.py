#!/usr/bin/env python3
"""Tiny smoke check for the native HPX --diag contract.

Runs the native hpx_synthetic_baseline binary twice on a small synthetic
workload and asserts the opt-in diagnostic behaves as designed:

  diag OFF -> exit 0, normal JSONL parses, NO <out>.diag.json is written.
  diag ON  -> exit 0, normal JSONL parses, <out>.diag.json is valid JSON with
              schema "diag-1" and the expected phase/queue/lane fields.

It does not assert numeric values (those are workload/timing dependent); it
locks the *shape* of the contract so a future change that drops .diag.json,
breaks its JSON, or perturbs the normal JSONL gets caught.

Usage:
    python bench/smoke_diag.py [--bin PATH]

--bin defaults to hpx_impl/build/hpx_synthetic_baseline (relative to repo root).
Exit code 0 == pass, 1 == fail.
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_BIN = os.path.join(REPO_ROOT, "hpx_impl", "build", "hpx_synthetic_baseline")

# Small, fast workload: a handful of short requests on two lanes.
BASE_ARGS = [
    "--service-ms", "1",
    "--requests", "20",
    "--concurrency", "4",
    "--num-lanes", "2",
]


def _fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)


def _run(binary, out_path, extra):
    cmd = [binary, *BASE_ARGS, "--out", out_path, *extra]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        _fail(f"binary exited {proc.returncode} for {extra or '(no diag)'}\n"
              f"  cmd: {' '.join(cmd)}\n  stderr: {proc.stderr.strip()}")
    return proc


def _parse_jsonl(path):
    if not os.path.exists(path):
        _fail(f"expected JSONL output missing: {path}")
    rows = []
    with open(path) as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                _fail(f"{path}:{n} is not valid JSON: {e}")
    if not rows:
        _fail(f"{path} parsed to zero rows")
    return rows


def check_diag_off(binary):
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "off.jsonl")
        _run(binary, out, [])
        rows = _parse_jsonl(out)
        # Normal schema must be untouched and must NOT leak diag-only fields.
        first = rows[0]
        if first.get("schema_version") != "1":
            _fail(f"diag-off: unexpected schema_version {first.get('schema_version')!r}")
        for leaked in ("enqueue_ns", "queue_depth_at_enqueue"):
            if leaked in first:
                _fail(f"diag-off: JSONL leaked diag-only field {leaked!r}")
        # No diag file at the default location.
        diag_path = out + ".diag.json"
        if os.path.exists(diag_path):
            _fail(f"diag-off: unexpected diag file written at {diag_path}")
    print("PASS: diag OFF -> normal JSONL only, no .diag.json")


WARNING_LINE = "[hpx_synthetic_baseline] warning: --diag-out ignored without --diag"


def check_diag_out_without_diag(binary):
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "warn.jsonl")
        diag_path = os.path.join(d, "should_not_exist.diag.json")
        # --diag-out provided but --diag NOT set: must warn, run normally, and
        # write no diag file.
        proc = _run(binary, out, ["--diag-out", diag_path])  # _run asserts exit 0
        # Exact warning line must appear on stderr.
        stderr_lines = [ln.strip() for ln in proc.stderr.splitlines()]
        if WARNING_LINE not in stderr_lines:
            _fail(f"diag-out-without-diag: expected exact warning line on stderr\n"
                  f"  want: {WARNING_LINE!r}\n  stderr: {proc.stderr.strip()!r}")
        # The requested diag file must NOT exist.
        if os.path.exists(diag_path):
            _fail(f"diag-out-without-diag: diag file written despite no --diag: {diag_path}")
        # Also no diag file at the default <out>.diag.json location.
        if os.path.exists(out + ".diag.json"):
            _fail(f"diag-out-without-diag: unexpected default diag file at {out}.diag.json")
        # Normal JSONL still valid schema "1" with no diag-only fields leaked.
        rows = _parse_jsonl(out)
        first = rows[0]
        if first.get("schema_version") != "1":
            _fail(f"diag-out-without-diag: unexpected schema_version "
                  f"{first.get('schema_version')!r}")
        for leaked in ("enqueue_ns", "queue_depth_at_enqueue"):
            if leaked in first:
                _fail(f"diag-out-without-diag: JSONL leaked diag-only field {leaked!r}")
    print("PASS: --diag-out without --diag -> warning, no diag file, normal JSONL intact")


def check_diag_on(binary):
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "on.jsonl")
        _run(binary, out, ["--diag"])
        # Normal JSONL still present and unchanged in shape.
        rows = _parse_jsonl(out)
        if rows[0].get("schema_version") != "1":
            _fail(f"diag-on: normal JSONL schema_version changed to "
                  f"{rows[0].get('schema_version')!r}")
        # Diag file at default <out>.diag.json.
        diag_path = out + ".diag.json"
        if not os.path.exists(diag_path):
            _fail(f"diag-on: expected diag file missing at {diag_path}")
        try:
            with open(diag_path) as f:
                diag = json.load(f)
        except json.JSONDecodeError as e:
            _fail(f"diag-on: {diag_path} is not valid JSON: {e}")
        if diag.get("schema") != "diag-1":
            _fail(f"diag-on: schema is {diag.get('schema')!r}, expected 'diag-1'")
        for key in ("phases_ms", "queue_depth_at_enqueue", "lanes", "config"):
            if key not in diag:
                _fail(f"diag-on: diag JSON missing top-level key {key!r}")
        for phase in ("push", "pickup", "service", "completion", "total"):
            if phase not in diag["phases_ms"]:
                _fail(f"diag-on: phases_ms missing {phase!r}")
            for stat in ("p50", "p90", "p99", "mean"):
                if stat not in diag["phases_ms"][phase]:
                    _fail(f"diag-on: phases_ms.{phase} missing {stat!r}")
        if not diag["lanes"]:
            _fail("diag-on: lanes array is empty")
        for la in diag["lanes"]:
            for key in ("actor_id", "processed", "busy_ms", "utilization"):
                if key not in la:
                    _fail(f"diag-on: lane entry missing {key!r}")
    print("PASS: diag ON -> diag-1 JSON with phases/queue/lanes, normal JSONL intact")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bin", default=DEFAULT_BIN,
                    help="path to hpx_synthetic_baseline (default: %(default)s)")
    args = ap.parse_args()
    if not os.path.exists(args.bin):
        _fail(f"binary not found: {args.bin} (build it first)")
    check_diag_off(args.bin)
    check_diag_out_without_diag(args.bin)
    check_diag_on(args.bin)
    print("OK: --diag smoke passed")


if __name__ == "__main__":
    main()
