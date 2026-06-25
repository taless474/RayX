# Ray bootstrap + endpoint identity seam — consolidated

> **Consolidated.** This note has been folded into the canonical
> [endpoint_runtime_seam.md](endpoint_runtime_seam.md) — see **§2 Endpoint identity seam**
> (and §1 for purpose/scope). The endpoint identity seam (minted `rtb-ep-<16 hex>`
> identities, the process-local registry, the peer-specific typed `int64` handshake, the
> closed/tombstone behavior, and the typed error taxonomy) is described there as stable
> design narrative.

This pointer stub is kept so existing links resolve. The original slice-by-slice text
(Slice 0 / 0.1, including the superseded resident-HPX endpoint mode) was working-tree-only
design provenance and is intentionally not retained in the consolidation; the durable
design lives in the canonical doc. An `Endpoint` is now HPX-free under the shared owner
(see [endpoint_runtime_seam.md §4](endpoint_runtime_seam.md)).
