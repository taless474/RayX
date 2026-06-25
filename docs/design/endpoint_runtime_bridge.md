# Endpoint → Runtime bridge (v1 / v2) — consolidated

> **Consolidated.** This note has been folded into the canonical
> [endpoint_runtime_seam.md](endpoint_runtime_seam.md) — see **§5 Endpoint → Runtime
> bridge**. The private/test-only `Connection._call_op(...)`, the fixed closed `CALL_OP`
> frame, the supported fixed ops (v1 `square`/`add`; v2 `fanout_sum`), the drain-gate
> state machine, the plain-`std::thread` accept thread, the `fut.get()` boundary, and the
> "parts-invariance is correctness, not HPX evidence" framing are described there as stable
> design narrative.

This pointer stub is kept so existing links resolve. The original v1 + v2 text — including
the wire/status tables, the dispatch refactor, the closeout validation runs, and the file
changelist — was working-tree-only design provenance and is intentionally not retained in
the consolidation; the durable design (including the wire frame, statuses, and op set)
lives in the canonical doc. The bridge is **local structural plumbing**, a
private/test-only seam with no public endpoint-call API; it is still native socket I/O —
not HPX socket serving, not HPX async socket I/O, not parcelport / AGAS, not multi-node —
and makes no HPX-value or performance claim. The corrected exp42/exp43 path interpretation
lives in [endpoint_runtime_seam.md §6](endpoint_runtime_seam.md).
