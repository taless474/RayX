# exp63 — HPX-native collective / tree reduction, then payload (experiment-only)

**Status: Slice 1b built and run on Rostam. Hardware evidence exists for the connector-lifetime race,
the Slice 2a native-composition retest, and the Slice 2b root-of-partials fan-in.** Both HPX-native
composed waits (`when_all_then_reduce`, `dataflow_reduce`) are cross-node validated for the closed-int64
mechanism slice under the hardened lifetime contract, and a depth-2 star / root-of-partials fan-in is
likewise validated under both collect waits (see the connector-lifetime, Slice 2a, and Slice 2b sections
below); the broader progress-sweep config matrix remains a further diagnostic. Mechanism / lifecycle /
topology evidence only — no performance, payload, Ray-comparison, same-axis, collective, or production
claim.
Slice 0 (pure-Python scaffold) and Slice 1a (progress config-matrix + gate/aggregate builders) are
complete and green. Slice 1b adds the experiment-only HPX C++ — `collective_ext` (embedded AGAS root
+ `exp63_leaf_action` fanout, the three composition modes, and the background-yielder progress probe)
and `collective_connector` (connect-mode remote locality) — copied/renamed from the proven exp62 C++
(`exp62`→`exp63`, no exp62 header included) plus the `run_progress_diagnosis` / `run_progress_sweep`
runner. The pure seams are unit-tested; the on-cluster orchestration and the CMake build are
**Rostam-only and UNVALIDATED off-cluster** (no local HPX/pybind11). Phases skip cleanly off-cluster.

exp63 is an experiment-only, same-axis Python-boundary measurement probe. It is **not** shipped
`rayx.runtime` API, **not** distributed RayX API, **not** an object store / `ObjectRef`, **not**
arbitrary remote Python execution, **not** Ray Serve, and **not** real inference. It computes no
Ray-vs-HPX ratio or speedup and uses no winner language.

## Why exp63 exists — the two exp62 caveats

exp62 Slice 4b is the current strongest same-axis distributed evidence, but it carries two caveats
this experiment targets:

1. **Interim composition.** exp62's cross-node HPX fan-in used `root_flat_gather_poll`
   (`composition_primitive=root_flat_gather_reduce`, `watchdog=bounded_is_ready_poll_50us`,
   `hpx_native_composition=false`). It is proven cross-node (job 158817) but not HPX-native. The
   native `hpx::when_all(...).then(reduce)` spike (job 158814) was mathematically correct yet the
   composed future **stalled to the dispatch timeout (~30 s) cross-node** and the run failed. The
   diagnosed cause is a **parcelport background-progress / passive-future-readiness** problem: a
   passive wait was not woken promptly by parcelport completion; the poll makes progress only because
   a yielding thread lets completion handlers run.
2. **Closed-int64-only payload.** One scalar per leaf, QD1 outer, no payload-size / serialization
   evidence.

## HPX-expert review outcome (direction for exp63)

The plan to start from `hpx::collectives::reduce` was **downgraded**:

* Collectives ride the **same** future + TCP-parcelport substrate as the stalled `when_all().then()`
  spike, so they may **reproduce the same passive-wait stall** rather than fix it. The claim that
  collectives "self-drive cross-locality progress" is a hypothesis to test, not a premise.
* Collectives are also **more fragile**: fixed participant membership, communicator generations, no
  built-in timeout, and a missing/crashed participant hangs forever and can **poison the island**
  (whole-island external restart, echoing exp50/exp51).
* The real first variable is **parcelport background progress**, not the composition primitive. A
  tree of `hpx::async` actions is equally HPX-native; "collective" is not the only native option.

### Revised order

* **Slice 0 (this):** pure-Python scaffold — oracles, provenance/gate builders, off-cluster-skipping
  phase stubs, selftests. No C++/Ray/HPX/Rostam.
* **Slice 1:** progress root-cause micro-slice on Rostam — reproduce the 158814 passive stall, then
  sweep runtime variables (root/connector `--hpx:threads`, parcelport background progress, MPI
  parcelport). Decisive de-risk. A pass means a **passive** wait wakes cross-node **without** a
  success-path poll. `root_flat_gather_poll` stays as the known-good control.
* **Slice 2a:** if progress is fixable, retest the existing `when_all_then_reduce` / `dataflow_reduce`
  native modes cross-node — may retire the poll with no new mechanism.
* **Slice 2b:** `tree_of_partials` (per-locality partial-sum actions reduced at root) as the
  likely-robust native path.
* **Later:** `hpx::collectives::reduce` only after progress + membership + timeout design is
  understood.
* **Later still:** payload — small fixed vector, then synthetic logits-like top-k. Composition and
  payload never change in the same slice.

## Slice 0 contents

`run_exp63_collective.py` (pure Python, no runtime imports at load):

* Closed-int64 **scalar oracle** (`leaf_value`, `composite_oracle`) — placement-independent by
  construction so a separate witness proves distribution.
* **Vector-sum** and **top-k** oracle **stubs** (`vector_sum_oracle`, `topk_oracle`) — deterministic,
  synthetic, not wired to execution, not model output.
* **Composition provenance** with a fail-closed consistency gate over the four candidate primitives
  (`root_flat_gather_poll`, `when_all_then_reduce`, `tree_of_partials`, `hpx_collective_reduce`).
  `hpx_native == not polled_in_success_path`; only the exp62-proven poll control may claim
  `cross_node_composition_validated`.
* **Progress-diagnosis** provenance (Slice 1) — `passive_progress_ok` is True only when a passive
  wait woke within budget **without** a success-path poll.
* **Payload provenance** — `payload_shape/len/bytes` plus the always-on
  `payload_is_synthetic` / `payload_not_model_output` / `no_inference` flags.
* **Distribution witness + gate** — ≥2 remote localities, all declared remotes covered, every
  participant contributed.
* **Hard fences** locked False (`speedup_computed`, `ratio_reported`, `arms_differenced`,
  `placement_bands_differenced`); no artifact sets `same_axis_comparison` True in Slice 0.
* CLI `--phase {selftest, smoke, progress-diagnosis, hpx-collective-local-smoke,
  hpx-collective-remote-smoke, tree-of-partials-remote-smoke}`; only `selftest` does real work, every
  runtime phase skips cleanly (off-cluster or ext-unbuilt).

`selftest_slice0.py` — hermetic pure-Python selftests for all of the above.

## Validation (Slice 0)

* `python3 -m py_compile run_exp63_collective.py selftest_slice0.py`
* `python3 selftest_slice0.py`
* `python3 run_exp63_collective.py --phase selftest`

## A1 connector-lifetime race and hardening

The A1 `when_all_then_reduce` "before_dispatch `std::system_error` / Operation not permitted" fault was
diagnosed as a connector serve-window / lifecycle race (zero EPERM syscalls; connector-side
`invalid_status: thread pool is not running`) and fixed with a heartbeat-deadman + root-completion
connector lifetime. See [`connector_lifetime_hardening.md`](connector_lifetime_hardening.md) for the
diagnosis, serve-timeout sweep, and same-allocation hardened-vs-control A/B. Mechanism/lifecycle
evidence only.

## Slice 2a: native composition retest (hardened lifetime)

With connector lifetime corrected, Slice 2a re-tested the two existing HPX-native composed-wait modes
end to end (`native-composition-smoke` phase). On job 159167 (medusa[06-08], root medusa06, connectors
medusa07/medusa08, N=8 all-remote 4/4, K=20, `serve-timeout=90` deadman, hardened heartbeat/root-
completion lifetime), both `when_all_then_reduce` and `dataflow_reduce` completed 20/20 with
`wait_for_status=ready`, correct closed-int64 oracle, all leaves remote, and both connectors leaving via
`root_completion_signal` — `cross_node_composition_validated=True`. The `root_flat_gather_poll` control
passed the same mechanics but is not native-validated (`polled_in_success_path=True`). See
[`native_composition_retest.md`](native_composition_retest.md) for the full per-mode table, the
old-fault → hardening → retest thread, and claim discipline. Mechanism/lifecycle evidence only; no
performance, payload, Ray-comparison, same-axis, or collective claim.

## Slice 2b: depth-2 star / root-of-partials fan-in (hardened lifetime)

Slice 2b changes the fan-in TOPOLOGY, not the wait primitive: each remote locality folds its own
contiguous leaf block locally and returns ONE partial, so the root composes r remote PARTIAL futures
instead of N leaf futures (flat native = N leaf futures; root-of-partials = r partial futures). It is a
depth-2 STAR / root-of-partials, NOT a k-ary tree, and it rides the already-validated Slice 2a native
wait. The r partials are collected with hand-rolled per-locality action futures (no fixed communicator,
no membership state, exceptions propagate through futures) rather than `hpx::collectives::reduce` —
lower risk under connect-mode dynamic membership; collectives stay later. On job 159200 (medusa[11-13],
root medusa11, connectors medusa12/medusa13, N=8 all-remote, contiguous blocks [4,4], K=20,
`serve-timeout=90` deadman, hardened lifetime), both collect waits — `dataflow_reduce` and
`when_all_then_reduce` — completed 20/20 with `wait_for_status=ready`, correct closed-int64 oracle, 2
partials on localities [1,2] tiling [0,8) exactly once, correct local partial oracles, and both
connectors leaving via `root_completion_signal`: `cross_node_composition_validated=True`. The flat
native controls also validated; the `root_flat_gather_poll` control passed mechanics but stays not
native-validated. See [`tree_of_partials_retest.md`](tree_of_partials_retest.md) for the full per-mode
table, the honest-topology / hand-rolled-vs-collectives framing, and claim discipline. Mechanism /
topology evidence only; not a new progress proof; no performance, payload, Ray-comparison, same-axis, or
collective claim.

## Claim discipline

Experiment-only; not shipped `rayx.runtime`; not distributed RayX API; not `ObjectRef` / object
store; not arbitrary Python execution; not Ray Serve; not real inference. No ratios, speedups, or
winner language.
