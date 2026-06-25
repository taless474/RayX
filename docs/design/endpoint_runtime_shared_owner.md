# Shared process-level HPX owner (Variant 2) — consolidated

> **Consolidated.** This note has been folded into the canonical
> [endpoint_runtime_seam.md](endpoint_runtime_seam.md) — see **§4 Shared owner model
> (Variant 2)**. The `ProcessHpxOwner` coexistence policy (`Engine` exclusive, `Runtime`
> singleton/HPX-owner, `Endpoint` multiple/HPX-free), the thread-topology consequences, the
> order-independent teardown, and the `_process_hpx_active()` diagnostic gate are described
> there as stable design narrative.

This pointer stub is kept so existing links resolve. The original text — including the
"Current problem" provenance for the pre-owner either/or state, the Variant 1 vs Variant 2
comparison, and the per-slice acceptance criteria — was working-tree-only design provenance
and is intentionally not retained in the consolidation; the durable design lives in the
canonical doc. The endpoint layer is HPX-free and the `Runtime` owns HPX; no HPX socket serving, parcelport,
AGAS, or multi-node is implied (see [endpoint_runtime_seam.md §8](endpoint_runtime_seam.md)).
