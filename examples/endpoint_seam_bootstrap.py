#!/usr/bin/env python3
"""Example: Ray bootstrap + endpoint seam + A1 transport (experimental).

A small runnable demonstration (NOT a benchmark, NO timing, NO performance claim):

  * Ray creates/hosts two actors; each mints one ``rayx.endpoint.Endpoint(transport=True)``
    with a stable opaque identity (``rtb-ep-<16 hex>``).
  * The driver reads each actor's endpoint metadata and hands each peer's metadata to the
    other -- bootstrap travels through Ray/Python as plain dicts ONLY.
  * Each actor proves the seam works inside its own process (peer-specific typed int64
    self-ping).
  * Actor A delivers a CROSS-PROCESS ping to actor B over the A1 transport (plain native
    local IPC, AF_UNIX); the value equals B's local-path transform.

A1 is PLAIN NATIVE LOCAL IPC on ONE node: NOT a fabric / parcelport / AGAS, NOT HPX
transport / HPX serving, NOT multi-node, NO performance claim. HPX is not the delivery
mechanism (the ping is computed inline). Ray is used only for actor placement/bootstrap,
never as a measured data path.

Run (requires `ray` and the built `_rayx`):
    python examples/endpoint_seam_bootstrap.py
"""
import os
import sys

os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
if RAYX_SRC not in sys.path:
    sys.path.insert(0, RAYX_SRC)


class EndpointHostActor:
    """A Ray actor hosting exactly one rayx.endpoint.Endpoint."""

    def __init__(self, rayx_src):
        if rayx_src not in sys.path:
            sys.path.insert(0, rayx_src)
        from rayx.endpoint import Endpoint
        self.ep = Endpoint(transport=True)

    def metadata(self):
        return self.ep.metadata()

    def self_ping(self, nonce):
        """Prove the seam works inside THIS actor process: connect to our own endpoint
        and ping it. Returns the peer-specific transform value."""
        from rayx.endpoint import connect
        conn = connect(self.ep.metadata())
        try:
            return conn.ping(nonce)
        finally:
            conn.close()

    def ping_peer(self, peer_meta, nonce):
        """Deliver a cross-process ping to a peer in ANOTHER process over the A1
        transport. Returns (outcome, value_or_detail)."""
        from rayx.endpoint import connect, EndpointError
        try:
            conn = connect(peer_meta)
            try:
                return ("ok", conn.ping(nonce))
            finally:
                conn.close()
        except EndpointError as e:
            return ("error", repr(e))

    def close(self):
        self.ep.close()
        return True


def main():
    import ray
    RemoteActor = ray.remote(EndpointHostActor)
    ray.init(num_cpus=2, include_dashboard=False,
             ignore_reinit_error=True, logging_level="error")
    try:
        a = RemoteActor.remote(RAYX_SRC)
        b = RemoteActor.remote(RAYX_SRC)

        meta_a = ray.get(a.metadata.remote())
        meta_b = ray.get(b.metadata.remote())
        print("actor A endpoint:", meta_a["endpoint_id"])
        print("actor B endpoint:", meta_b["endpoint_id"])
        print("distinct identities:", meta_a["endpoint_id"] != meta_b["endpoint_id"])

        # In-process seam works inside each Ray-hosted actor; responses are peer-specific.
        pa = ray.get(a.self_ping.remote(100))
        pb = ray.get(b.self_ping.remote(100))
        print("A self-ping(100):", pa)
        print("B self-ping(100):", pb)
        print("peer-specific (A != B for same nonce):", pa != pb)

        # Cross-process ping A -> B is DELIVERED over the A1 plain-IPC transport.
        from rayx.endpoint._validate import endpoint_id_hash
        PING_XOR = 0x52415958
        nonce = 777
        outcome, val = ray.get(a.ping_peer.remote(meta_b, nonce))
        expected_b = nonce ^ PING_XOR ^ endpoint_id_hash(meta_b["endpoint_id"])
        print(f"A -> B cross-process ping({nonce}): {outcome} -> {val}")
        print("  matches B's local transform:", val == expected_b)

        ray.get(a.close.remote())
        ray.get(b.close.remote())
        print("clean teardown OK")
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
