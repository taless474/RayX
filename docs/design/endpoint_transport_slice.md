# Endpoint transport slice — A1 (consolidated)

> **Consolidated.** This note has been folded into the canonical
> [endpoint_runtime_seam.md](endpoint_runtime_seam.md) — see **§3 Local endpoint transport
> (A1)**. The A1 transport (opt-in `Endpoint(transport=True)`, one process-local AF_UNIX
> listener, owner-only socket dir, the fixed 39-byte/16-byte PING frame, one-shot
> dial-per-call probed-handle semantics, the invariance property, and the A1.1 robustness
> hardening) is described there as stable design narrative.

This pointer stub is kept so existing links resolve. The original A1 + A1.1 text, including
per-run validation numbers and the file changelist, was working-tree-only design provenance
and is intentionally not retained in the consolidation; the durable design lives in the
canonical doc. A1 is plain native AF_UNIX IPC — **not** HPX transport, **not** HPX serving, **not** HPX async
I/O, **not** a fabric, **not** parcelport / AGAS, **not** multi-node — and the future
distributed-fabric direction remains gated (see
[endpoint_runtime_seam.md §8](endpoint_runtime_seam.md)).
