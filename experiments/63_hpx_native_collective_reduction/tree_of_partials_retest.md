# exp63 — Slice 2b: depth-2 star / root-of-partials fan-in under the hardened connector-lifetime contract

**Status:** mechanism / topology evidence. Not performance evidence, and NOT a new progress proof.
HPX-only, closed-int64 only. No Ray, no payload, no collectives, no same-axis comparison, no
production-runtime claim. No ratio / speedup / winner language. Fences (`speedup_computed`,
`ratio_reported`, `arms_differenced`, `placement_bands_differenced`) locked False;
`same_axis_comparison` False.

Slice 2a validated the two existing HPX-native composed waits (`when_all_then_reduce`,
`dataflow_reduce`) cross-node under the hardened lifetime contract (see
[`native_composition_retest.md`](native_composition_retest.md)). Slice 2b changes the fan-in
**topology**, not the wait primitive: instead of the root composing N leaf futures, each remote locality
folds its own contiguous leaf block **locally** and returns ONE partial, and the root composes the r
remote **partial** futures. It rides the **already-validated Slice 2a native-wait substrate**, so it is
a topology/fan-in-structure slice, not a fresh progress experiment.

## Topology (honest naming)

This is a **depth-2 star / root-of-partials**, **not** a general k-ary tree. There are no intermediate
combiners and no log-depth hierarchy: the root fans out one partial action per remote locality and
reduces the r returned partials directly. The structural point is that root fan-in is bounded by
**remote-locality count**, not leaf count:

* flat native composes **N** leaf futures at the root;
* root-of-partials composes **r** remote partial futures at the root;
* in this run N=8, r=2, so each remote locality folds **4** leaves locally and returns one partial, and
  the root reduces **2** partials.

At N=8, r=2 this is mechanism/topology evidence, **not** a performance result — no latency, no ratio, no
differencing. The value is the structural shape (fewer parcels, edge-side combine), on record for later
larger-N-per-locality design, not a measured win here.

## Why hand-rolled action futures, not `hpx::collectives::reduce`

The r partials are collected with plain per-locality action futures composed by a validated native wait,
deliberately **not** `hpx::collectives::reduce`:

* no fixed communicator;
* no generation / membership state;
* one action future per remote locality;
* a failed / departed locality surfaces as an **exception through its future** (it does not hang a
  fixed-membership communicator);
* lower risk under connect-mode **dynamic membership** — the poison / whole-island-restart hazard that
  exp50 / exp51 documented for fixed-membership collectives does not apply.

Collectives remain **later** work, to be attempted only with explicit membership / generation / timeout /
poison-recovery handling. Nothing here validates collectives.

The success-path wait is a bounded harness watchdog (`composed_partial_future_wait_for`,
`future::wait_for(timeout)`), **not** an application-level poll; on timeout the outstanding partial
actions are abandoned in flight (HPX has no clean force-cancel) — an honest caveat, bounded by the
hardened connector lifetime.

## Local build/import sanity (Mac; no cross-node claim)

* Local HPX found at `$HOME/Desktop/Repos/hpx-install` (HPXConfig.cmake + hpx.hpp).
* Local CMake build succeeded against local HPX (pybind11 from the `_rayx` venv, python 3.11).
* `collective_connector` and `collective_ext.cpython-311-darwin.so` built.
* `collective_ext` imported successfully; `fanout_fanin_tree_remote_diag` symbol present.
* A pre-start call raised the expected `HPX not started` guard (no crash).
* This is compile / import / smoke sanity only — **no cross-node evidence** from the Mac.

## Result (job 159200)

Fresh 3-node `medusa` allocation, `--exclusive`, subnet `10.42.5.x`. Root = medusa11 (embedded AGAS
root, `-c 8`); connectors = medusa12 / medusa13 (remote localities 1, 2); N=8 all-remote, contiguous
blocks [4, 4]. Settings: N=8, prewarm=5, K=20, `dispatch-timeout-s=8.0`, `serve-timeout=90` (deadman),
`await-timeout=60`, `n-remote=2`, hardened heartbeat / root-completion connector lifetime enabled. Each
mode ran on a fresh root port; artifact filenames encode job + mode/collect-wait + serve-timeout + port
(no fixed-filename clobbering).

### Root-of-partials modes

| collect wait | verdict | calls | wait_for_status | exception | oracle | topology | partials | leaf counts | localities | coverage-once | local partials | shutdown reason | stayed_alive | late_parcel | cross_node_composition_validated |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dataflow_reduce` | **PASS** | 20/20 | ready | none | correct | `depth2_star_of_partials_contiguous_blocks` | 2 | [4, 4] | [1, 2] | True | True | root_completion_signal ×2 | True | False | **True** |
| `when_all_then_reduce` | **PASS** | 20/20 | ready | none | correct | `depth2_star_of_partials_contiguous_blocks` | 2 | [4, 4] | [1, 2] | True | True | root_completion_signal ×2 | True | False | **True** |

Both root-of-partials modes additionally reported `hpx_native_composition=True`,
`polled_in_success_path=False`, `all_remote_localities_contributed=True`, `root_reduces_partial_count=2`,
`no_dispatch_timeout=True`, `timed_out_partial_count=0`, `short_circuited=False`, watchdog
`composed_partial_future_wait_for`, `node_set=[medusa11, medusa12, medusa13]`,
`remote_locality_ids=[1, 2]`, fences locked False, and `same_axis_comparison=False`.

### Flat-native controls (same allocation, structural correlation only — no differencing)

| mode | verdict | calls | cross_node_composition_validated |
| --- | --- | --- | --- |
| flat `dataflow_reduce` | PASS | 20/20 | **True** |
| flat `when_all_then_reduce` | PASS | 20/20 | **True** |
| flat `root_flat_gather_poll` (control) | mechanics pass | 20/20 | **False** |

All controls: `wait_for_status=ready`, oracle correct, `no_dispatch_timeout=True`, both connectors
`root_completion_signal`, `late_parcel=False`. The poll control satisfies the mechanics gates but reports
`polled_in_success_path=True`, so its `cross_node_composition_validated` is False by construction — it is
a known-good mechanics control / fallback, not native validation.

## Interpretation

* Slice 2b **validates the depth-2 root-of-partials fan-in topology cross-node** under the hardened
  connector-lifetime contract: for both supported native collect waits (`dataflow_reduce`,
  `when_all_then_reduce`), 20/20 calls ready, correct closed-int64 oracle, r=2 partials on the two remote
  localities with blocks [4, 4] tiling [0, 8) exactly once, each local partial oracle correct, and both
  connectors leaving via `root_completion_signal` with no late parcel.
* It **strengthens the exp63 native-composition arc** by adding a structural fan-in path that bounds root
  fan-in by remote-locality count (r partial futures) rather than leaf count (N leaf futures), on top of
  the flat native modes already validated in Slice 2a.
* This is **not a new progress proof**: it rides the same native-wait + TCP-parcelport substrate already
  validated in Slice 2a.
* `root_flat_gather_poll` remains a useful mechanics control / fallback, but it is not native-validated
  because it uses success-path polling.

## What this does and does not license (claim discipline)

* Experiment-only; not shipped `rayx.runtime`; not distributed RayX API; not `ObjectRef` / object store;
  not arbitrary Python execution; not Ray Serve; not real inference.
* Validation is **under the hardened lifetime contract** for a closed-int64 mechanism/topology slice at
  N=8, r=2. It establishes **no** performance, payload behavior, Ray comparison, same-axis evidence,
  collectives behavior, robustness under all timings / transports / scales, or production-runtime
  behavior.
* No ratios, no speedups, no winner language. The only comparison stated is structural (N leaf futures
  vs r partial futures), and it is **not** a measured performance result.

## Roadmap impact

* **Roadmap strengthened.** exp63 now has **both** flat native composition (`when_all_then_reduce`,
  `dataflow_reduce`) **and** a depth-2 root-of-partials structural fan-in validated cross-node under the
  corrected connector-lifetime contract.

## Next recommended step

* Pause exp63 at this clean boundary; **or**
* later design collectives (`hpx::collectives::reduce`) only with explicit membership / generation /
  timeout / poison-recovery handling (not a drop-in for the hand-rolled fan-in);
* the immediate active project thread can return to the **exp64 Ray matched smoke** if desired.

## Artifacts

Gitignored under `_exp63_runs/slice2b_tree_copyback_159200/` (copied back from Rostam; source not synced
back):

* `exp63_treesmoke_159200_dataflowreduce_st90_p7930_hpx.json`
* `exp63_treesmoke_159200_whenallthenreduce_st90_p7932_hpx.json`
* `exp63_nativesmoke_159200_dataflowreduce_st90_p7934_hpx.json`
* `exp63_nativesmoke_159200_whenallthenreduce_st90_p7936_hpx.json`
* `exp63_nativesmoke_159200_rootflatgatherpoll_st90_p7938_hpx.json`
* five per-mode tee logs (`tree_dataflow_159200.log`, `tree_whenall_159200.log`,
  `flat_dataflow_159200.log`, `flat_whenall_159200.log`, `flat_poll_159200.log`)

## Reproduce

```
python -u run_exp63_collective.py --phase tree-of-partials-remote-smoke \
  --partial-collect-wait {dataflow_reduce|when_all_then_reduce} \
  --n 8 --prewarm 5 --k 20 --dispatch-timeout-s 8.0 --serve-timeout 90 \
  --await-timeout 60 --n-remote 2 --prefer-subnet 10.42.5. --root-port <fresh-port>
```

Rostam-only (needs a ≥3-node allocation + the built `collective_ext` / `collective_connector`); the
phase skips cleanly off-cluster or when the ext / connector is unbuilt. The driver runs as the root step
on the first allocated compute node with the full allocation nodelist restored in its environment and
srun-launches the two connectors onto the other two nodes.
