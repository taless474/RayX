# exp63 — Slice 2a: native composition retest under the hardened connector-lifetime contract

**Status:** mechanism / lifecycle evidence. Not performance evidence. HPX-only, closed-int64 only. No
Ray, no payload, no collectives, no same-axis comparison, no production-runtime claim. No ratio /
speedup / winner language. Fences (`speedup_computed`, `ratio_reported`, `arms_differenced`,
`placement_bands_differenced`) locked False; `same_axis_comparison` False.

Slice 2a re-tests the two existing HPX-native composed-wait modes now that the connector serve-window /
lifecycle race is fixed (see [`connector_lifetime_hardening.md`](connector_lifetime_hardening.md)). It
asks one question: under a **correct** connector lifetime — connectors alive for the whole dispatch
window — does a native composed wait (`when_all_then_reduce` / `dataflow_reduce`) actually complete
cross-node for the closed-int64 mechanism slice (all K calls ready, oracle correct, all leaves remote,
connectors leaving via root completion)? If so, the mode is promoted from "diagnostic / UNVALIDATED
cross-node" to "cross-node validated **under the hardened lifetime contract**".

## What changed from the earlier diagnosis

Three steps connect the old fault to this retest:

1. **Old fault.** The native `when_all_then_reduce` spike (exp62 job 158814) stalled the composed future
   to the dispatch timeout cross-node; a later instrumented A1 run surfaced a `before_dispatch`
   `std::system_error` / "Operation not permitted". It read like an intrinsic native progress/permission
   failure.
2. **Connector-lifetime hardening.** The fault was diagnosed as a connector serve-window race: a fixed
   wall-clock `serve-timeout` let a connector's HPX pool stop while the root was still dispatching, so a
   leaf parcel hit `create_work` on a stopped pool (`invalid_status: thread pool is not running`). The
   fix makes `serve-timeout` a deadman guard: the root heartbeats before every dispatch and writes a
   completion sentinel only after all calls, so connectors stay alive until root completion. The
   same-allocation A/B (job 159061) proved it: hardened passed 20/20 at `serve-timeout=90`, the
   no-completion control reproduced the original call-7 fault.
3. **Slice 2a retest (this report).** With the corrected lifetime contract, the native modes are
   re-tested end to end under a positive validation gate, not just an anomaly probe.

The upshot: the earlier native failure was **dominated by the connector serve-window / lifecycle race**,
not by an intrinsic inability of `when_all_then_reduce` or `dataflow_reduce` to make progress cross-node
in this TCP setup.

## Result (job 159167)

Fresh 3-node `medusa` allocation, `--exclusive`, subnet `10.42.5.x`. Root = medusa06 (embedded AGAS
root, `-c 8`); connectors = medusa07 / medusa08 (remote localities 1, 2); all N=8 leaves remote, 4/4
across the two remote localities. Settings: N=8, prewarm=5, K=20, `dispatch-timeout-s=8.0`,
`serve-timeout=90` (deadman), `await-timeout=60`, `n-remote=2`, hardened heartbeat / root-completion
connector lifetime enabled. Each mode ran on a fresh root port; per-mode artifact filenames encode
job + mode + serve-timeout + port (no fixed-filename clobbering).

| mode | verdict | calls | wait_for_status | exception | oracle | connector shutdown reason | stayed_alive | late_parcel | cross_node_composition_validated |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `when_all_then_reduce` | **PASS** | 20/20 | ready | none | correct | root_completion_signal ×2 | True | False | **True** |
| `dataflow_reduce` | **PASS** | 20/20 | ready | none | correct | root_completion_signal ×2 | True | False | **True** |
| `root_flat_gather_poll` (control) | mechanics pass | 20/20 | ready | none | correct | root_completion_signal ×2 | True | False | **False** |

All three modes additionally reported `no_dispatch_timeout=True`, `timed_out_leaf_count=0`,
`short_circuited=False`, `mechanics_ok=True`, `connector_lifetime_mode="heartbeat_root_completion"`,
placement `node_set=[medusa06, medusa07, medusa08]`, `remote_locality_ids=[1, 2]`, fences locked False,
and `same_axis_comparison=False`.

The poll control (`root_flat_gather_poll`) satisfies every mechanics gate but reports
`polled_in_success_path=True`, so its `cross_node_composition_validated` is **False** by construction:
it is a known-good control, not native validation. Its `hpx_native_composition=False`; the two native
modes report `hpx_native_composition=True` / `polled_in_success_path=False`.

## Interpretation

* Under the corrected connector-lifetime contract, **both** existing HPX-native composed waits validate
  cross-node for this closed-int64 mechanism slice: `when_all_then_reduce` and `dataflow_reduce` each
  completed 20/20 with `wait_for_status=ready`, correct closed-int64 oracle, all leaves remote across two
  localities, and both connectors leaving via `root_completion_signal` with no late parcel.
* This is **mechanism / lifecycle evidence** that the earlier native failure was dominated by the
  connector serve-window / lifecycle race, not an intrinsic inability of `when_all_then_reduce` or
  `dataflow_reduce` to progress cross-node in this TCP setup.
* `root_flat_gather_poll` remains a useful control and the proven interim cross-node path, but it is
  **not** native-validated because it uses success-path polling.
* `when_all_then_reduce` and `dataflow_reduce` are now **promotable** for the next closed-int64 mechanism
  slice under the hardened lifetime contract.

## What this does and does not license (claim discipline)

* Experiment-only; not shipped `rayx.runtime`; not distributed RayX API; not `ObjectRef` / object store;
  not arbitrary Python execution; not Ray Serve; not real inference.
* Native validation here is **under the hardened lifetime contract** for a closed-int64 mechanism slice.
  It does **not** claim native composition is robust under all timings, transports, or scales.
* Collectives (`hpx::collectives::reduce`) remain **later** work; nothing here validates collectives, and
  their membership / timeout / poison hazards are unaddressed.
* This establishes **no** performance, payload, Ray-comparison, same-axis, or production-runtime behavior.
* No ratios, no speedups, no winner language. `root_flat_gather_poll` is a control, never a native pass.

## Roadmap impact

* **Roadmap strengthened.** Native composition is back on the viable path once connector lifetime is
  correct: both `when_all_then_reduce` and `dataflow_reduce` are cross-node validated for the closed-int64
  mechanism slice under the hardened lifetime contract, so the native-composition direction no longer
  depends on the interim poll to make cross-node progress in this setup.

## Next recommended step

Design **Slice 2b: `tree_of_partials`** (per-locality partial-sum actions reduced at the root) as the
likely-robust native path, measured beside the now-validated `when_all_then_reduce` / `dataflow_reduce`
and the `root_flat_gather_poll` control, still HPX-only closed-int64 mechanism scope under the hardened
lifetime contract. Composition and payload never change in the same slice; payload and collectives stay
later.

## Artifacts

Gitignored under `_exp63_runs/slice2a_native_retest_copyback_159167/` (copied back from Rostam; source
not synced back):

* `exp63_nativesmoke_159167_whenallthenreduce_st90_p7920_hpx.json`
* `exp63_nativesmoke_159167_dataflowreduce_st90_p7922_hpx.json`
* `exp63_nativesmoke_159167_rootflatgatherpoll_st90_p7924_hpx.json`
* three per-mode tee logs (`whenallthenreduce_159167.log`, `dataflowreduce_159167.log`,
  `rootflatgatherpoll_159167.log`)

## Reproduce

```
python -u run_exp63_collective.py --phase native-composition-smoke \
  --composition-mode {when_all_then_reduce|dataflow_reduce|root_flat_gather_poll} \
  --n 8 --prewarm 5 --k 20 --dispatch-timeout-s 8.0 --serve-timeout 90 \
  --await-timeout 60 --n-remote 2 --prefer-subnet 10.42.5. --root-port <fresh-port>
```

Rostam-only (needs a ≥3-node allocation + the built `collective_ext` / `collective_connector`); the
phase skips cleanly off-cluster or when the ext / connector is unbuilt. The driver runs as the root step
on the first allocated compute node and srun-launches the two connectors onto the other two nodes.
