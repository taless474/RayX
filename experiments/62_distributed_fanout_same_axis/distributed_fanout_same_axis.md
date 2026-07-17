# exp62 — Same-axis Python-boundary distributed fanout/fanin: Ray vs experiment-only Python→HPX

Durable experiment-local account of exp62: the extension of the exp61 same-axis discipline
from one scalar remote call to a **distributed fanout/fan-in workload**, measured for both
runtimes at the same Python caller boundary under matched R=5 bands with hard no-ratio
fences.

---

## 1. Executive summary

**Problem.** exp61 proved the same-axis measurement discipline on a single QD1 scalar
remote call. A single-RTT micro-call cannot speak to distributed behavior: fanout across
localities, fan-in reduction, and multi-node placement.

**Role in the roadmap.** exp62 carries the same boundary, harness, matched-band, and fence
discipline into the first genuinely distributed same-axis workload, and en route surfaced
the HPX-native composition problem that exp63 later resolved.

**Answer.** The headline evidence is **Slice 4b**: a matched R=5 cross-node band on a
three-node topology (coordinator/root runs zero leaves; N=8 leaves split 4/4 across two
remote localities/nodes on both arms), all gates passed, measured at the identical Python
caller boundary:

| arm | call primitive | p50 | p90 | p99 | mean |
|---|---|---|---|---|---|
| Ray coordinator/task path | `ray.get(coordinator.remote(x, N))` | 3717.4 µs | 3874.4 µs | 7012.5 µs | 3805.4 µs |
| experiment-only Python→HPX action path | `ext.fanout_fanin_remote(x, N)` (poll) | 249.5 µs | 270.7 µs | 320.2 µs | 251.5 µs |

The bands are reported side by side only — never differenced, ratioed, or ranked.

**Limitations.** Synthetic closed-`int64` leaves, QD1 at the outer boundary, N=8, one
allocation family on Rostam; the HPX fan-in uses a proven interim polled composition (not
an HPX-native collective); no payload axis (exp64), no concurrency axis (exp69); the
Python→HPX binding is experiment-only, not shipped `rayx.runtime` API.

---

## 2. Motivation

One outer blocking Python call per timed iteration:

```text
fanout_fanin(x, N) -> int64
```

internally dispatches **N leaf actions to remote placement** and reduces them to one
closed-`int64` value. The workload stays QD1 at the caller boundary, but each call performs
real distributed fanout and fan-in, so a band can no longer be attributed to a single-RTT
artifact.

The oracle is deliberately **placement-independent** so it can never masquerade as a
placement proof:

```text
leaf(x, i)      = (x ^ 0x52415958) + (i << 1)
composite(x, N) = Σ leaf(x, i) for i in [0, N)     (int64, order-independent)
```

Distribution is proven separately: per-leaf locality witnesses, hard placement gates,
attested transport, and node/locality IDs.

---

## 3. Workload and mechanism

**Ray arm.** A coordinator hard-pinned to the head node with `num_cpus=0` (the head
advertises zero CPUs — control-plane only) submits N leaf tasks hard-pinned
(`NodeAffinitySchedulingStrategy(soft=False)`) round-robin across the remote worker nodes,
then reduces. One caller-boundary submission per timed iteration; `ray.get` is bounded so a
placement failure fails closed instead of hanging.

**HPX arm (experiment-only).** The embedded root locality (exp61 embedding pattern) fans
N registered C++ leaf actions across the joined remote connect-mode localities over the TCP
parcelport and reduces in C++. The proven cross-node fan-in composition is
`root_flat_gather_poll`: a bounded fine-grained `is_ready` poll (50 µs yield) over the
individual leaf futures — unready futures are never `get()`'d, and any timed-out leaf fails
the gates closed.

**Shared measurement contract** (from exp61): same Python caller boundary,
`perf_counter_ns`, matched K=1000 / W=100 / prewarm=1, per-call composite-oracle check,
matched `N`, placement mode, node set, and subnet; strict off-cluster skip; fences
`speedup_computed` / `ratio_reported` / `arms_differenced` /
`placement_bands_differenced` hard-locked false.

---

## 4. Methodology: the slice ladder

| slice | question retired | outcome |
|---|---|---|
| 0 | pure-Python scaffold: oracle, witnesses, gates, fences | pass |
| 1 | local single-locality HPX fanout mechanism | pass (can never claim same-axis) |
| 2 | HPX one-remote dry-run over TCP | pass (mechanism only; cpuset caveat) |
| 3 | matched cross-node R=5 band, **one** remote locality | **accepted**, superseded by 4b |
| 4a | HPX-only fanout across **two** remote localities | pass (mechanism only, no Ray arm) |
| — | HPX-native composition spike (`when_all_then_reduce`) | **informative negative** (see §6) |
| 4b | matched R=5 band, ≥2 remote localities/nodes, both arms | **accepted — headline** |

Matched-band gates mirror exp61 Slice 4/5 and add distribution witnesses: every leaf lands
remotely (`leaves_local=0`, `leaves_remote=N`, `witness_leaf_count=N`), every remote
locality/node receives its assigned share (4/4 split at N=8), connector cpusets are
attested non-collapsed (8 distinct CPUs), TCP_NODELAY is verified on live sockets, no
dispatch timeouts, full connector join→serve→graceful-disconnect lifecycle, and cross-island
config agreement. `same_axis_comparison=true` is earned purely structurally and carries no
ratio, speedup, ranking, or winner semantics.

---

## 5. Accepted results

### Slice 4b — matched multi-remote band (headline)

Three-node `--exclusive` allocation: root / Ray head / coordinator on **medusa00** (zero
leaves in both arms); remote connectors / Ray workers on **medusa01** and **medusa02**;
subnet `10.42.5.`; N=8 split 4/4; R=5. All five HPX islands ran first and fully down before
the Ray arm. All gates passed; both arms composite-oracle-correct (`11040115504`) on every
island. Per-arm bands (across-island medians of per-island percentiles) are the table in
§1.

### Slice 3 — one-remote predecessor band (superseded)

Two-node allocation (medusa00 → medusa01), all N=8 leaves to a single remote
locality/node, R=5, all gates passed:

| arm | p50 | p90 | p99 | mean |
|---|---|---|---|---|
| Ray actor/task path | 3640.9 µs | 3895.6 µs | 6407.0 µs | 3718.6 µs |
| experiment-only Python→HPX action path | 345.4 µs | 401.7 µs | 466.2 µs | 359.0 µs |

Slice 3 remains valid one-remote-locality evidence; Slice 4b supersedes it as the headline
because both arms genuinely span two remote localities/nodes.

### Slice 4a — HPX-only multi-remote mechanism (no Ray arm)

Three-node dry-run proving the ≥2-remote-locality HPX path end-to-end: two connectors
(localities 1 and 2) each received 4 of 8 leaves, composite oracle correct, verified
NODELAY, attested 8-CPU cpusets, full lifecycle, zero timeouts, all 17 gates true.
`same_axis_comparison=false` — mechanism evidence only.

---

## 6. Engineering finding: the HPX-native composition negative

Before Slice 4b, an HPX-expert review flagged that the polled fan-in is not HPX-native. A
spike added selectable composition modes (`when_all_then_reduce` via
`hpx::when_all(...).then(reduce)`; `dataflow_reduce` via `hpx::dataflow`) with honest
composition provenance and a fail-closed consistency gate.

Cross-node, the native `when_all_then_reduce` mode was **mathematically correct** (correct
composite, correct 4/4 distribution) but the composed future **stalled to the ~30 s
dispatch timeout on every call** — a cross-node readiness/progress issue in this setup: the
passive wait was not woken promptly by parcelport completion, while the polling path made
progress because the yielding thread let completion handlers run. The native modes were
kept in-tree but gated off; `root_flat_gather_poll` remained the proven composition and the
Slice 4b basis.

This negative is what exp63 subsequently resolved: the stall was traced to **connector
lifetime**, not an intrinsic HPX progress failure, and hardened connector
completion/heartbeat lifetime validated `when_all_then_reduce` and `dataflow_reduce`
cross-node. Within exp62's accepted evidence, the polled composition remains what was
measured.

Two smaller mid-run lessons (fixed and validated before the accepted band): a coordinator
hard-pinned to a zero-CPU head node must be submitted with `num_cpus=0` or it pends
forever, and `ray.get` must be bounded so placement failures fail closed; an early
`when_all().wait_for()` watchdog turned an all-leaves-complete call into a full-timeout
block (~30 s → ~340 µs after reverting to the bounded poll); an orphan-scan false positive
(matching its own `srun` wrapper) was corrected to filter the detector's own command line.

---

## 7. Interpretation

- **Same-axis extends to distributed shape.** The exp61 discipline survives a real
  fanout/fan-in workload with multi-remote placement on both arms, matched topology, and
  full structural gating.
- **Same-axis means same boundary and matched topology, not same internals.** The HPX arm
  is a registered C++ action fanned over the TCP parcelport and reduced in C++; the Ray arm
  is task scheduling plus object-store transport with a Python-driver coordinator. That is
  why the bands are reported separately with no ratio.
- **Composition matters and was not yet native.** The measured HPX fan-in is the interim
  polled gather. exp63 later validated native composition after connector-lifetime
  hardening; exp62's numbers should not be read as characterizing HPX-native collectives.
- Magnitudes are Rostam-allocation-specific and TCP-parcelport-specific,
  observation-only.

---

## 8. Limitations and non-claims

- Synthetic closed-`int64` leaves; QD1 at the outer boundary; N=8 only; no payload-size
  axis, no QD>1/pipeline regime, not real serving or inference.
- No ratio, speedup, difference, ranking, or winner — in either direction; the fences are
  locked false in every curated artifact.
- Slice 4a and the composition spike are HPX-only mechanism evidence, not comparisons.
- The Python→HPX binding is experiment-only under `experiments/`; the shipped
  `rayx.runtime` API gained no distributed actions.
- No production, fault-tolerance, elasticity, or scaling claim.

---

## 9. Relationship to neighboring experiments

- **exp61** (predecessor): the scalar QD1 same-axis band and the measurement contract exp62
  reuses.
- **exp63** (successor, mechanism): resolves this experiment's native-composition negative
  via connector-lifetime hardening.
- **exp64** (successor, axis): adds the payload-size fan-in axis this experiment lacks.
- **exp69** (successor, regime): adds verified-workload latency/throughput comparison under
  matched resources; exp62's band remains the strongest *closed-int64 fanout/fanin*
  same-axis evidence.

---

## 10. Evidence and reproducibility

**Accepted and diagnostic jobs (all Rostam, subnet `10.42.5.`):**

| slice | Slurm job | nodes | status |
|---|---|---|---|
| 3 (one-remote band) | **158809** | medusa00 → medusa01 | accepted, superseded by 4b |
| 4a (HPX-only multi-remote) | **158813** | medusa00 + medusa01/medusa02 | pass, mechanism-only |
| composition spike | **158814** | medusa00 + medusa01/medusa02 | informative negative |
| 4b (matched multi-remote band) | **158817** | medusa00 + medusa01/medusa02 | **accepted headline** |

**Tracked curated artifacts:**

- `exp62_fanout_band_158809_aggregate.json` + `exp62_fanout_band-158809_i{1..5}_manifest.json`
  (Slice 3);
- `exp62_fanout_mrband_158817_aggregate.json` + `exp62_fanout_mrband_158817_i{1..5}_manifest.json`
  (Slice 4b).

Slice 4a and the spike have no separately tracked curated artifact; they are captured in
this write-up, the evidence index, and gitignored copybacks.

**Gitignored raw evidence:** per-island raw arm artifacts
(`exp62_fanout_*_{hpx,ray}.json`), per-connector bootstrap dirs (`attest_connect.json` +
`joined1`/`served1.ok`/`disconnected1` lifecycle markers), Ray head/worker logs, and batch
logs under `_exp62_runs/` (`band_copyback_158809/`, `slice4a_copyback/`,
`mrband_full_copyback_158817/`); build outputs (`build/`, the `fanout_ext` `.so`,
`fanout_connector`).

**Sources (tracked):** `shared_fanout.hpp`, `fanout_ext.cpp`, `fanout_action.hpp`,
`fanout_connector.cpp`, `run_exp62_fanout.py`, `CMakeLists.txt`, `.gitignore`.

**Resource-shape provenance (accepted bands):** connectors launched with
`--cpus-per-task=8 --cpu-bind=mask_cpu --hpx:threads=8 --hpx:bind=none`; attested effective
cpuset `[0,2,4,6,8,10,12,14]` on every island (not collapsed); Ray head `--num-cpus 0`;
coordinator `num_cpus=0` hard-pinned to the head node; leaves hard-pinned to worker nodes.
Orphan-freedom: post-run scans clean (Slice 3/4b); Slice 4a relies on graceful-disconnect
lifecycle witnesses plus clean Slurm teardown (compute nodes unreachable after the
allocation ended).

**Fences (locked false in every curated artifact):** `speedup_computed`, `ratio_reported`,
`arms_differenced`, `placement_bands_differenced`.
