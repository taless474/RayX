# exp61 — Same-axis Python-boundary comparison: Ray actor vs experiment-only Python→HPX action

Durable experiment-local account of exp61: the first RayX experiment to time a Ray actor
call and an experiment-only Python→HPX action at the **same Python caller boundary**, under
matched runs, with hard no-ratio fences.

---

## 1. Executive summary

**Problem.** Earlier evidence measured the two runtimes at different boundaries: exp58 timed
the HPX two-node path from **C++** (`hpx::async(...).get()`), while exp59 timed the Ray
actor path from **Python** (`ray.get(...)`). Those are different caller boundaries, so
exp58/exp59 is a plane-labeled juxtaposition, not a comparison.

**Role in the roadmap.** exp61 establishes the measurement discipline every later same-axis
experiment (exp62, exp64, exp69) builds on: one shared Python timing harness, one clock, one
caller boundary, per-call value oracles, matched allocations, and artifact fences that keep
`speedup_computed` / `ratio_reported` / `arms_differenced` hard-locked false.

**Answer.** Both arms can be driven through one identical QD1 harness at the identical
Python caller boundary, on two nodes and on one node, with every gate passing. The accepted
evidence is two R=5 matched bands:

- **cross-node band** (medusa00 → medusa01): Ray actor path p50 ≈ **518 µs**;
  experiment-only Python→HPX action path p50 ≈ **185 µs** — reported side by side, never
  differenced or ratioed;
- **same-node control band** (medusa00 only): Ray p50 ≈ **519 µs**; Python→HPX p50 ≈
  **93 µs** — a placement control isolating physical co-location within each arm.

**Limitations.** One scalar closed-`int64` micro-call at queue depth 1. Observation-only,
machine-specific bands; no ratio, winner, or general Ray-vs-HPX claim is licensed; the
experiment-only binding adds nothing to the shipped `rayx.runtime` API.

---

## 2. Motivation: the measurement-boundary problem

A credible Ray-vs-HPX statement requires both mechanisms to be observed from the same place.
exp61 fixes the observation point at the Python caller:

```text
t0 = perf_counter_ns()
result = <blocking op>          # Ray:      ray.get(actor.dist_probe.remote(x))
                                # HPX/RayX: ext.dist_probe_remote(x)   (experiment-only)
t1 = perf_counter_ns()
```

Both arms share the closed-`int64` oracle family of exp58:
`result = (x ^ 0x52415958) + (node_tag << 1)`, where `node_tag` is the HPX `locality_id` on
the HPX arm and an explicit recorded tag on the Ray arm. Every call is value-checked; an
incorrect result invalidates the run.

---

## 3. Mechanism: two arms, one harness

**Shared harness.** `timed_qd1(call, x, expected, k, w)` drives any blocking callable: one
prewarm, `W` dropped warmups, `K` timed `perf_counter_ns` samples, per-call oracle check.
QD1 (one in-flight call), blocking-only, no pipelining. Both arms use the same `K`/`W`/
prewarm/clock, so the arms cannot drift into different timing semantics.

**Ray arm.** A genuine remote Ray actor timed via
`ray.get(actor.dist_probe.remote(x))`. Cross-node: hard-pinned off the driver node with
`NodeAffinitySchedulingStrategy(soft=False)` and verified off-node before timing. Same-node
control: hard-pinned to the driver's own node and verified on-node. Never an in-process
call.

**HPX arm (experiment-only).** A pybind11 extension (`dist_probe_ext.cpp`) embeds an HPX
runtime in the Python process as the AGAS root (`hpx::start`, GIL released around blocking
calls); a standalone connect-mode locality (`dist_probe_connector.cpp`) joins over the TCP
parcelport and serves the same registered action. The timed call
`ext.dist_probe_remote(x)` dispatches to the **cached remote locality** — never `find_here`,
never a single-node fallback (the call refuses if no remote has joined). Same-node control:
still two distinct localities over loopback TCP — the parcelport software path is held
constant and only the physical NIC/switch hop is removed.

**Strict skip discipline.** Off-cluster or missing prerequisites produce a clean SKIP (exit
0, no curated aggregate); failures write ignored `*_fail.json` siblings. A local run can
never fabricate a two-node result.

---

## 4. Methodology: the slice ladder and gates

exp61 was built as a gated ladder; each slice retired one risk before the next:

| slice | question retired | outcome |
|---|---|---|
| 0 | can Python embed an HPX runtime and get a correct action result back? | pass (single-locality smoke) |
| 1 | can the Ray arm run at the identical boundary through the same harness? | pass (local actor smoke) |
| 2A | can two-node code exist that *always* skips cleanly off-cluster? | pass (scaffolding + strict skip) |
| 2B | do both arms actually work across two nodes? | pass — two independent mechanism proofs |
| 3 | can both arms run in **one** allocation with proven-matched conditions? | pass (pair manifest, `matched_structure_validated`) |
| 4 | R=5 matched **cross-node** band at the same boundary | **accepted** (headline evidence) |
| 5 | R=5 matched **same-node** control band | **accepted** (placement control) |

**Matched-band gates (Slices 4/5).** `same_axis_comparison` becomes true only if: R ≥ 5;
every island's manifest and both arm artifacts pass; every manifest
`matched_structure_validated=true`; both arms exercised on the intended placement; both
oracles correct on every call; cross-island agreement on job / node pair / subnet /
K / W / prewarm / clock / boundary; and a captured clock overhead. Regardless of outcome,
`speedup_computed = ratio_reported = arms_differenced = false` are hard-locked, and Slice 5
additionally locks `placement_bands_differenced = false`.

**Same-node HPX validity gates (Slice 5).** Two fail-closed witnesses ensure the same-node
band measures placement, not a configuration artifact: `disjoint_core_binding_verified`
(effective — not requested — CPU affinity on both localities, non-overlapping cpusets) and
`hpx_tcp_nodelay_verified` (real `getsockopt(TCP_NODELAY)` attestation on the connector's
live parcelport sockets to the root, never a config assumption).

---

## 5. Accepted results

### Cross-node band (Slice 4; medusa00 → medusa01, subnet 10.42.5.)

R=5 matched islands, both arms at K=1000 / W=100 / prewarm=1, `perf_counter_ns` (median
clock overhead 92 ns), all gates passed, `overall=pass`,
`comparison_kind=r5_matched_same_axis_band_no_ratio`.

Per-arm RTT (across-island median of each island's percentiles; the two arms are summarized
separately and never differenced):

| arm | call primitive | p50 | p90 | p99 | mean |
|---|---|---|---|---|---|
| Ray actor path | `ray.get(actor.dist_probe.remote(x))` | ~518.3 µs | ~850.7 µs | ~1125.7 µs | ~584.7 µs |
| experiment-only Python→HPX action path | `ext.dist_probe_remote(x)` | ~184.8 µs | ~257.5 µs | ~322.6 µs | ~188.7 µs |

### Same-node control band (Slice 5; medusa00, corrected resource shape)

R=5 matched islands on one exclusive node; HPX root 4 threads on cpuset 0–3, connector 4
threads on cpuset 4–7 (enforced + verified disjoint); TCP_NODELAY attested on 8 live
parcelport sockets per island; Ray actor verified on the driver node (5/5 islands); no Ray
head co-resident during the HPX arms. All gates passed, `overall=pass`,
`comparison_kind=r5_matched_same_node_band_no_ratio` (clock overhead median 83 ns).

| arm | call primitive | p50 | p90 | p99 | mean |
|---|---|---|---|---|---|
| Ray actor path | `ray.get(actor.dist_probe.remote(x))` | ~519.1 µs | ~790.9 µs | ~1028.6 µs | ~559.0 µs |
| experiment-only Python→HPX action path | `ext.dist_probe_remote(x)` | ~93.0 µs | ~102.3 µs | ~112.9 µs | ~94.1 µs |

Observation-only: with comparable per-locality resources the HPX same-node arm is tight and
consistent across islands (per-island p99 ~108–127 µs); one island recorded a single
isolated ~34 ms max spike on one call out of 1000 (p99 unaffected).

---

## 6. Interpretation

- **The same-axis discipline works.** Both mechanisms can be observed from the identical
  Python caller boundary under matched conditions, with per-call value validation — the
  methodological foundation for exp62/64/69.
- **Placement moved one arm and not the other.** Within the HPX arm, removing the physical
  network hop moved the band (p50 ~185 µs cross-node → ~93 µs same-node); within the Ray
  arm the band was essentially unchanged (~518 vs ~519 µs p50). This is a within-arm
  placement observation, consistent with the expectation that a QD1 micro-call is dominated
  by per-call software path costs rather than the wire for the Ray path at this scale. It is
  not a cross-arm statement.
- **QD1 micro-call scope.** These bands characterize the smallest possible remote
  operation. They say nothing about payload sizes (exp64), fanout composition (exp62/63),
  or concurrency/throughput regimes (exp69).

---

## 7. Engineering findings

Summarized from the development and audit history (details preserved in §10):

- **Slurm step-scoped detection can mask an allocation** (Bug A): a one-node `srun` step of
  a two-node job sees a one-node environment; the fix consults
  `scontrol show job` and takes the largest node count across signals.
- **An embedded HPX root under Slurm needs `--hpx:ignore-batch-env`** (Bug B): HPX otherwise
  builds its node list from Slurm hostname batch variables and rejects explicit IP
  endpoints; connectors must self-bind their own node's selected-subnet IP
  (`--prefer-subnet`), not the root's IP.
- **Resource shape can masquerade as a placement effect:** the first same-node band showed a
  high quantized HPX tail that an audit traced to `--cpu-bind=map_cpu` binding the connector
  to a single core (one HPX worker thread → idle-backoff wakeup latency), not to loopback.
  The corrected mask-based binding plus explicit `--hpx:threads` collapsed the tail
  (p99 ~112 µs). This is the earliest form of the resource-supply lesson that exp69 Slice 3
  later formalized.
- **Verification must be effective, not requested:** the disjoint-affinity and TCP_NODELAY
  gates pass only on observed kernel state (`sched_getaffinity`, `getsockopt` on live
  sockets), and fail closed on any doubt.

---

## 8. Limitations and non-claims

- One scalar closed-`int64` micro-call; QD1 only; no payload, fanout, or concurrency axis.
- Bands are observation-only and machine-specific (Rostam medusa00/medusa01).
- The two arms are reported side by side only: no ratio, no difference, no winner, no
  general "HPX beats Ray" / "Ray is slower" claim; the same-node and cross-node bands are
  never differenced against each other.
- The Python→HPX binding is experiment-only; the shipped `rayx.runtime` API gained no
  distributed actions.
- No production, fault-tolerance, inference, object-store, or scaling claim.

---

## 9. Relationship to neighboring experiments

- **exp58/exp59** (predecessors): per-plane measurements that motivated the same-axis
  requirement; superseded as comparison framing by exp61.
- **exp60**: within-HPX same-node/cross-node decomposition; exp61 Slice 5 is the same-axis
  analog of that control.
- **exp62**: extends the same-axis discipline from one scalar call to distributed
  fanout/fan-in (its Slice 4b reuses this boundary and fence structure).
- **exp69**: the eventual gated performance comparison for a useful workload; its matched
  resource bands are the mature version of the Slice 5 resource-shape lesson.

---

## 10. Evidence and reproducibility

**Accepted jobs:**

| slice | Slurm job | topology | result |
|---|---|---|---|
| 2B (mechanism proofs, independent runs) | — (manual runs) | medusa00 → medusa01 | both arms pass |
| 4 (cross-node band) | **158724** | medusa00 → medusa01, subnet 10.42.5. | `overall=pass`, R=5 |
| 5 (same-node control band) | **158734** | medusa00 (exclusive) | `overall=pass`, R=5 |

**Diagnostic/superseded jobs:** **158732** — first same-node band; structurally valid
mechanism pass, timing band confounded by the single-core connector binding
(`map_cpu`); its numbers are not used. **158733** — HPX-only probe confirming the corrected
binding collapsed the tail. **158731** — cancelled attempt; a Ray head co-resident during
the HPX phase coincided with a connector crash/root hang; accepted runs keep the phases
separate. A bounded dispatch-side timeout (turning a connector crash into a clean per-island
failure) remains a noted robustness follow-up.

**Tracked curated artifacts:**

- `hpx_smoke_aggregate.json`, `ray_smoke_aggregate.json` (Slices 0/1);
- `hpx_connected_aggregate.json`, `ray_remote_aggregate.json` (Slice 2B);
- `slice3_band_158724_i{1..5}_manifest.json` and `slice4_band_158724_aggregate.json`
  (Slice 4);
- `slice5_sn_sn_band2_158734_i{1..5}_manifest.json` and
  `slice5_samenode_band_158734_aggregate.json` (Slice 5).

**Gitignored raw artifacts:** pair-scoped per-arm JSON siblings
(`slice3_*_{hpx,ray}.json`, `slice5_sn_*_{hpx,ray}.json`), `_exp61_runs/`, `build/`,
`*.so`, logs, and redirected/skip/fail siblings.

**Sources (tracked):** `shared_dist_probe.hpp` (action + oracle), `dist_probe_ext.cpp`
(embedding + root/remote API), `dist_probe_connector.cpp` (connect-mode locality),
`run_exp61_same_axis.py` (phases incl. `selftest`), `CMakeLists.txt`, `.gitignore`.

**Fences in every artifact:** `same_axis_comparison` only via full gate passage;
`speedup_computed=false`, `ratio_reported=false`, `arms_differenced=false`, and (Slice 5)
`placement_bands_differenced=false` — hard-locked.
