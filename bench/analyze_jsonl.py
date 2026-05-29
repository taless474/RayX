"""Analyzer: read a per-request JSONL file and print an aggregate summary.

Emits the aggregate summary schema from docs/experiment_plan.md as a single
JSON object on stdout (and optionally to --out).

Example:
  python bench/analyze_jsonl.py results/ray_sleep5_c8.jsonl
"""

import argparse
import json
import sys

SUMMARY_SCHEMA_VERSION = "1"


def _percentile(sorted_vals, q):
    """Nearest-rank percentile (q in [0,100]) over a pre-sorted list."""
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    # Linear interpolation between closest ranks.
    rank = (q / 100.0) * (len(sorted_vals) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = rank - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


def _pcts(vals):
    s = sorted(vals)
    return {
        "p50": _percentile(s, 50),
        "p90": _percentile(s, 90),
        "p99": _percentile(s, 99),
    }


def load(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _queue_wait_note(boundary):
    """Boundary-aware caveat for the queue_wait_ms field.

    queue_wait_ms means different things depending on how it was measured:
      * ray-actor-process: client and actor live in different processes, so the
        client cannot read the actor's clock. queue_wait_ms is approximated as
        total_ms - service_ms_observed and includes all non-service time
        (Python, Ray runtime, process/IPC, serialization), not just queueing.
      * hpx-intra-locality: single process, one monotonic steady clock, so
        queue_wait_ms = start_ns - submit_ns is an exact submit-to-start time.
      * unknown/mixed: stay neutral.
    """
    if boundary == "ray-actor-process":
        return (
            f"boundary={boundary}; total_ms is client-side authoritative; "
            "queue_wait_ms is approximate (= total_ms - service_ms_observed) "
            "and includes all non-service time across the Ray process/runtime "
            "boundary, not just queueing."
        )
    if boundary == "hpx-intra-locality":
        return (
            f"boundary={boundary}; total_ms is client-side authoritative; "
            "queue_wait_ms is exact same-process steady-clock submit-to-start "
            "time (= start_ns - submit_ns)."
        )
    return (
        f"boundary={boundary}; total_ms is client-side authoritative; "
        "queue_wait_ms interpretation depends on the boundary (exact for "
        "same-process runs, approximate across process boundaries)."
    )


def summarize(rows):
    if not rows:
        raise ValueError("no rows to summarize")

    first = rows[0]
    completed_rows = [r for r in rows if r.get("status") == "completed"]
    failed = sum(1 for r in rows if r.get("status") == "failed")

    total_ms = [r["total_ms"] for r in completed_rows]
    queue_wait_ms = [r["queue_wait_ms"] for r in completed_rows]
    service_ms = [r["service_ms_observed"] for r in completed_rows]

    # Throughput over the observed span of the run (submit -> result),
    # reconstructed from client submit_ns and the end of service.
    if completed_rows:
        span_start = min(r["submit_ns"] for r in completed_rows)
        span_end = max(r["submit_ns"] + r["total_ms"] * 1e6 for r in completed_rows)
        span_s = max((span_end - span_start) / 1e9, 1e-9)
        throughput = len(completed_rows) / span_s
    else:
        throughput = 0.0

    total_p = _pcts(total_ms)
    qw_p = _pcts(queue_wait_ms)
    svc_p = _pcts(service_ms)

    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "run_id": first.get("run_id"),
        "backend": first.get("backend"),
        "boundary": first.get("boundary"),
        "workload": first.get("workload"),
        "num_requests": len(rows),
        "completed": len(completed_rows),
        "failed": failed,
        "throughput_req_s": throughput,
        "total_ms_p50": total_p["p50"],
        "total_ms_p90": total_p["p90"],
        "total_ms_p99": total_p["p99"],
        "queue_wait_ms_p50": qw_p["p50"],
        "queue_wait_ms_p90": qw_p["p90"],
        "queue_wait_ms_p99": qw_p["p99"],
        "service_ms_p50": svc_p["p50"],
        "service_ms_p90": svc_p["p90"],
        "service_ms_p99": svc_p["p99"],
        "total_ms_min": min(total_ms) if total_ms else None,
        "total_ms_max": max(total_ms) if total_ms else None,
        "notes": _queue_wait_note(first.get("boundary")),
    }


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Aggregate a per-request JSONL run.")
    p.add_argument("path", help="Input JSONL file.")
    p.add_argument("--out", default=None, help="Optional path to write summary JSON.")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    rows = load(args.path)
    summary = summarize(rows)
    text = json.dumps(summary, indent=2)
    print(text)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
