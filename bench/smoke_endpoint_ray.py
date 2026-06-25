#!/usr/bin/env python3
"""Ray-hosted structural smoke for the endpoint seam + A1 transport.

Smoke-only, NO timing, NO performance claim: two Ray actors each host one
`rayx.endpoint.Endpoint(transport=True)`. Proves structurally that:
  * two Ray-hosted actors mint DISTINCT endpoint identities (rtb-ep-<16 hex>);
  * endpoint metadata is exchanged through Ray/Python (plain dicts) for bootstrap ONLY;
  * each actor validates the peer metadata;
  * the seam works INSIDE a Ray-hosted process (self-handshake -> peer-specific int64
    transform, so two actors' self-pings differ for the same nonce);
  * a CROSS-PROCESS ping (actor A -> actor B) is DELIVERED over the A1 plain-IPC
    (AF_UNIX) transport and returns the same peer-specific transform as the local path;
  * clean teardown.

A1 is PLAIN NATIVE LOCAL IPC on ONE node: NOT a fabric / parcelport / AGAS, NOT HPX
transport / HPX serving, NOT multi-node. HPX is not the delivery mechanism. Ray is used
only for actor placement/bootstrap, never as a measured data path. No timing is asserted.

Skips cleanly (exit 0) if either `ray` or the built `_rayx` extension is unavailable.
On a failed assertion it prints FAIL and exits 1.

Usage:
    python bench/smoke_endpoint_ray.py
Exit code 0 == pass or skip, 1 == fail.
"""
import os
import sys

os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAYX_SRC = os.path.join(REPO_ROOT, "python", "src")
for _p in (REPO_ROOT, RAYX_SRC):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# The fixed handshake transform key (mirror endpoint_seam.hpp ENDPOINT_PING_XOR).
PING_XOR = 0x52415958
ENDPOINT_ID_PREFIX = "rtb-ep-"


# ray is imported and the class decorated inside main() so importing this module does
# NOT require ray (keeps the skip-clean check authoritative).
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
        from rayx.endpoint import connect
        conn = connect(self.ep.metadata())
        try:
            return conn.ping(nonce)
        finally:
            conn.close()

    def validate_peer(self, peer_meta):
        """Validate peer metadata only (bootstrap check). Returns (ok, detail)."""
        from rayx.endpoint._validate import validate_metadata
        try:
            validate_metadata(peer_meta)
            return (True, None)
        except Exception as exc:  # noqa: BLE001
            return (False, repr(exc))

    def ping_peer(self, peer_meta, nonce):
        """Deliver a cross-process ping to a peer over the A1 transport. Returns
        (outcome, value_or_detail)."""
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


def _fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)


def _skip(msg):
    print(f"SKIP: {msg}")
    sys.exit(0)


def main():
    try:
        import ray  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        _skip(f"ray not importable: {exc}")
    try:
        import rayx  # noqa: F401  (needs the built _rayx extension)
    except Exception as exc:  # noqa: BLE001
        _skip(f"rayx/_rayx not importable (build the extension first): {exc}")

    import ray
    RemoteActor = ray.remote(EndpointHostActor)
    ray.init(num_cpus=2, include_dashboard=False,
             ignore_reinit_error=True, logging_level="error")
    try:
        a = RemoteActor.remote(RAYX_SRC)
        b = RemoteActor.remote(RAYX_SRC)

        # (1) Distinct identities, valid format.
        meta_a = ray.get(a.metadata.remote())
        meta_b = ray.get(b.metadata.remote())
        for who, m in (("A", meta_a), ("B", meta_b)):
            if not m["endpoint_id"].startswith(ENDPOINT_ID_PREFIX):
                _fail(f"actor {who} endpoint id {m['endpoint_id']!r} bad prefix")
            if set(m) != {"endpoint_id", "pid", "host", "proto_version",
                          "transport_kind", "transport_addr"}:
                _fail(f"actor {who} metadata keys wrong: {sorted(m)}")
            if m["transport_kind"] != "unix" or not m["transport_addr"]:
                _fail(f"actor {who} transport not enabled: {m['transport_kind']!r}")
        if meta_a["endpoint_id"] == meta_b["endpoint_id"]:
            _fail("the two actors minted the SAME endpoint id")
        print(f"PASS: distinct identities A={meta_a['endpoint_id']} "
              f"B={meta_b['endpoint_id']}")

        # (2) Ray-bootstrapped peer metadata validates on each side.
        ok_a, det_a = ray.get(a.validate_peer.remote(meta_b))
        ok_b, det_b = ray.get(b.validate_peer.remote(meta_a))
        if not (ok_a and ok_b):
            _fail(f"peer metadata failed validation: A={det_a} B={det_b}")
        print("PASS: Ray-bootstrapped peer metadata validates on both sides")

        # (3) In-process seam works inside each Ray-hosted actor; the response is
        # PEER-SPECIFIC (mixes the endpoint's own id hash), so A's and B's self-pings
        # differ for the same nonce.
        from rayx.endpoint._validate import endpoint_id_hash
        nonce = 100
        results = {}
        for who, actor, meta in (("A", a, meta_a), ("B", b, meta_b)):
            v = ray.get(actor.self_ping.remote(nonce))
            expected = nonce ^ PING_XOR ^ endpoint_id_hash(meta["endpoint_id"])
            if v != expected:
                _fail(f"actor {who} self_ping({nonce})={v}, expected {expected}")
            results[who] = v
        if results["A"] == results["B"]:
            _fail("self_ping not peer-specific: A and B gave the same response")
        print("PASS: in-process self-handshake works inside each Ray-hosted actor "
              "(peer-specific: A != B)")

        # (4) Cross-process ping A -> B is DELIVERED over the A1 transport and matches
        # the same peer-specific transform as B's local path.
        nonce_x = 777
        outcome, val = ray.get(a.ping_peer.remote(meta_b, nonce_x))
        expected_b = nonce_x ^ PING_XOR ^ endpoint_id_hash(meta_b["endpoint_id"])
        if outcome != "ok":
            _fail(f"A->B cross-process ping outcome {outcome!r} (detail={val})")
        if val != expected_b:
            _fail(f"A->B cross-process ping={val}, expected {expected_b}")
        print("PASS: A->B cross-process ping delivered over A1 transport "
              "(value matches peer transform)")

        # (5) Clean teardown.
        if not (ray.get(a.close.remote()) and ray.get(b.close.remote())):
            _fail("actor close() did not return True")
        print("PASS: clean teardown")

        print("ALL PASS: endpoint seam + A1 transport (Ray-hosted smoke)")
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
