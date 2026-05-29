"""Ray actor baseline: a synthetic serving backend behind a single Ray actor.

This module defines the opaque synthetic backend and the Ray actor that hosts
it. It does not drive load or write output -- see bench/run_ray_baseline.py.

Scope (per docs/experiment_plan.md):
  * Ray public APIs only, one actor.
  * work_mode "sleep" only; service_ms == 0 is the no-op / null-dispatch case.
  * No streaming, no cancellation.

Timing note: start_ns / end_ns are measured inside the actor process. They are
NOT cross-process-comparable with the client's submit_ns (different processes,
same machine). The client treats total_ms as authoritative; see the driver.
"""

import time
import uuid

import ray

SCHEMA_VERSION = "1"
BACKEND = "synthetic"
BOUNDARY = "ray-actor-process"

# Valid request fields, per the synthetic backend contract.
WORK_MODE_SLEEP = "sleep"


@ray.remote
class SyntheticServer:
    """A single long-lived actor that simulates an opaque inference backend.

    The actor performs no real work: for work_mode "sleep" it sleeps for the
    requested service_ms (0 == no-op). It returns server-side timing so the
    driver can record service time separately from round-trip latency.
    """

    def __init__(self):
        self.actor_id = "act-" + uuid.uuid4().hex[:8]

    def get_actor_id(self):
        return self.actor_id

    def serve(self, request):
        """Service one request and return server-side timing + status.

        request: dict with request_id, service_ms, chunks, chunk_delay_ms,
        work_mode. Returns a dict merged into the per-request record by the
        driver.
        """
        start_ns = time.perf_counter_ns()
        status = "completed"
        error = None
        service_ms_requested = request.get("service_ms", 0)
        work_mode = request.get("work_mode", WORK_MODE_SLEEP)
        try:
            if work_mode != WORK_MODE_SLEEP:
                raise ValueError(f"unsupported work_mode: {work_mode!r}")
            # service_ms == 0 is the degenerate no-op (null dispatch).
            if service_ms_requested and service_ms_requested > 0:
                time.sleep(service_ms_requested / 1000.0)
        except Exception as exc:  # noqa: BLE001 - record any backend failure
            status = "failed"
            error = repr(exc)
        end_ns = time.perf_counter_ns()
        return {
            "actor_id": self.actor_id,
            "start_ns": start_ns,
            "end_ns": end_ns,
            "service_ms_observed": (end_ns - start_ns) / 1e6,
            "status": status,
            "error": error,
        }


def make_request(request_id, service_ms, work_mode=WORK_MODE_SLEEP,
                 chunks=1, chunk_delay_ms=0):
    """Build a synthetic-backend request dict matching the contract."""
    return {
        "request_id": request_id,
        "service_ms": service_ms,
        "chunks": chunks,
        "chunk_delay_ms": chunk_delay_ms,
        "work_mode": work_mode,
    }
