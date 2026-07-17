# exp69 — Same-axis top-k orchestration performance (Slices 0–3)

Durable experiment-local account of exp69. This is the standing record of methodology,
evidence, scoped comparisons, causal resolution, and claim boundaries for Slices 0–3.

All numbers, job IDs, hashes, and topologies in this document were verified against the
four curated aggregates and the copied-back Rostam evidence in this directory.

---

## 1. Executive summary

exp69 compares **two peer-candidate orchestration mechanisms inside the same
Ray-hosted, HPX-resident actor topology** for one identical, deterministic,
vocabulary-sharded top-k request measured at the same Python caller boundary:

- **Ray-mediated arm** — the coordinator actor reaches its peer through a nested Ray
  actor call; the candidate payload returns over the Ray actor call path.
- **HPX-mediated arm** — the coordinator posts native work onto its HPX runtime, reaches
  its peer through an `hpx::async` action over the HPX TCP parcelport, and composes the
  reply with a native future/continuation before merging.

This is **not standalone Ray versus standalone HPX**. Both arms run inside the same
Ray-hosted, HPX-resident, hard-placed actor topology; the Ray arm is not a pure
standalone-Ray deployment, and the HPX arm shares that same runtime. The local top-k and
the merge algorithm are identical between arms and are correctness-gated bit-for-bit.

The arc:

- **Slice 1 — QD1 caller-observed latency.** Single-in-flight (queue depth 1) per-arm
  latency distributions for six payload cases, every timed sample verified bit-exactly.
- **Slice 2 — bounded-concurrency goodput and latency under load.** Closed-loop
  verified-completion goodput and latency-under-load at concurrency `C ∈ {2, 4}` with a
  bounded correctness verifier outside the timed intervals.
- **Slice 3 — causal resource decomposition.** A controlled resource-band matrix that
  isolates the cause of the Slice 2 P3b/C=4 direction reversal, with per-stage native
  timing decomposition.

Slice 3 resolves the Slice 2 P3b/C=4 reversal as a **thread-supply resource asymmetry**
in the accepted two-worker HPX configuration, with **no implementation defect**. Under
matched worker supply the two per-arm goodput distributions converged to the same
approximate region.

Interpretation classification:

```text
thread_supply_resource_asymmetry_supported
no_implementation_defect_observed
```

---

## 2. Exact call paths

Both accepted arms compute their **own-shard top-k before dispatching peer work**, so any
absence of own/peer overlap is symmetric between the arms and is not the cause of any
observed difference.

### Ray-mediated arm

```text
ray.get(coordinator.ray_coordinate.remote(...))          # outer Ray controller call (timed boundary)
  → own-shard native top-k (C++, GIL released)
  → nested Ray actor call to the peer coordinator          # caller-observed peer request
      → peer own-shard native top-k, returns candidate payload
  → native merge of own + peer candidates (C++, identical algorithm)
  → return to controller
```

The nested Ray call duration is **caller-observed**: it includes Ray queueing, transport,
peer service, and return. It is **not** pure network or transport time.

### HPX-mediated arm

```text
ray.get(coordinator.hpx_coordinate.remote(...))          # outer Ray controller call (timed boundary)
  → hpx::post of the coordinator body onto the local HPX runtime
  → own-shard native top-k on an HPX worker (GIL released)
  → hpx::async peer action over the HPX TCP parcelport     # peer request; candidate payload returns as a future
  → .then continuation on the coordinator's default HPX pool merges own + peer candidates
  → cooperative merged.get() on the HPX thread (UNTIMED, cooperative suspension)
  → promise fulfilled; caller bound by std::future::wait_for on the calling OS thread (GIL released)
  → return to controller
```

GIL behavior: native compute, merge, and the HPX/Ray waits all release the GIL. The
sustained macOS timed-wait crash is a **separate diagnostic finding** and does not alter
this measured path; the measured HPX arm uses `hpx::post` + action + `.then` + untimed
`merged.get()` bounded by an outer `std::future::wait_for`.

---

## 3. Workload and correctness

**Workload.** Deterministic, synthetic, vocabulary-sharded top-k over a closed integer/
float32 value model. It is LLM-*shaped* work only — see §15 for the non-claims.

**Value generation rule** (`int_grid_over_8`, float32-exact):

```text
h    = (uint32(token_id) * 2654435761 + uint32(seed) * 40503) mod 2^32
grid = (h % 131) - 65                     # integer grid in [-65, 65]
logit = float32(grid) / 8.0               # exact float32, value range [-8.125, 8.125]
```

**Total order:** higher logit wins; on equal logit, lower **global** token id wins
(stable tie-breaking across shards).

**Timed payload cases** (coordinator's own shard = `split`; peer holds the rest):

| case | V | split | k | purpose |
|---|---|---|---|---|
| P0  | 64 | 32 | 1 | fixed-overhead control |
| P1  | 2,000,000 | 1,000,000 | 10 | local-compute dominated |
| P2  | 200,000 | 100,000 | 10,000 | larger peer payload |
| P3a | 100,000 | 50,000 | 10 | balanced small list |
| P3b | 100,000 | 50,000 | 100 | balanced medium list |
| P3c | 100,000 | 50,000 | 1,000 | balanced larger list |

**Correctness pre-gate (7-case matrix)**, run before any timed block on each island:
`tiny_k1`, `tiny_k3`, `cross_both`, `tie_cutoff`, `shardA_dom`, `both_contrib`,
`k1_large`.

**Per-sample correctness (every timed sample, established strictly after the timing
boundary t1):** exact global token IDs, exact ordering, exact float32 bit patterns,
checked against an **independent Python oracle**. Invalid results never contribute a
timing sample.

**Mechanism-counter exclusivity:** per-arm witness counters confirm the Ray arm served
only Ray fetches and the HPX arm served only HPX actions (no cross-arm leakage).

**Root isolation:** the HPX root/controller is work-free; it supervises membership and
lifecycle but serves no application action on the measured path.

**Lifecycle and orphan gates:** hard Ray placement verified; membership reached three
roles on three nodes; graceful locality leave; clean root finalize; actors destroyed and
recreated; remote orphan checks ran with zero Ray or peer orphans.

---

## 4. Provenance and topology

Common runtime for all accepted runs:

- **HPX commit** `20bc3d4bf3068383edcb63be13f22e9ff95842fa` (HPX V2.0.0, AGAS V3.0, Boost
  1.91.0, Hwloc 2.12.0; release build dated Jul 13 2026). This commit carries the upstream
  timed-wait readiness fix.
- **Ray** 2.55.1.
- **Python** 3.12.3 (CPython, GIL-enabled, non-free-threaded).
- **Transport:** HPX arm over the **HPX TCP parcelport**; Ray arm over the Ray actor call
  path. Both cross-node.
- **Network:** per-node **`10.42.5.x`** interfaces (`eno16`), subnet prefix `10.42.5.`.
- **Placement:** hard Ray `NodeAffinitySchedulingStrategy(soft=False)`; Ray node id
  treated as authoritative.
- **Root:** work-free HPX root/controller.

Accepted jobs and topologies:

| Slice | stage | Slurm job | root / controller | actor A | actor B |
|---|---|---|---|---|---|
| 1 (QD1 latency) | accepted cross-node | **171408** | medusa00 (10.42.5.30) | medusa01 (10.42.5.31) | medusa11 (10.42.5.41) |
| 2 (throughput) | smoke | **171560** | medusa00 | medusa01 | medusa11 |
| 2 (throughput) | accepted | **171561** | medusa00 (10.42.5.30) | medusa01 (10.42.5.31) | medusa11 (10.42.5.41) |
| 3 (causal) | smoke | **172121** | medusa01 | medusa11 | medusa12 |
| 3 (causal) | accepted reproduction | **172122** | medusa01 | medusa11 | medusa12 |
| 3 (causal) | final curated accepted | **172125** | medusa01 (10.42.5.31) | medusa11 (10.42.5.41) | medusa12 (10.42.5.42) |

The Slice 3 accepted matrix used medusa[01,11,12] (three idle medusa nodes); the third
node is medusa12 rather than the earlier-slice medusa11-as-B, which is recorded in the
Slice 3 preflight. All roles remained on distinct nodes on the `10.42.5.x` subnet with
hard placement.

---

## 5. Slice 1 results (QD1 caller-observed latency)

**Design:** queue depth 1 (one in-flight request at a time), coordinator = actor A
(direction A) with a separate B-direction control never pooled with A. Sampling:
A = W=50 warmup / K=500 timed per case; B control = W=20 / K=200 on P0/P2/P3b only;
R=3 reps.

**Correctness / sample totals (accepted):**

- Direction A: **18,000** timed samples (9,000 per arm; P0–P3c × 500 × 3 reps).
- Direction B control: **3,600** timed samples (1,800 per arm; P0/P2/P3b × 200 × 3 reps),
  reported separately, never pooled with A.
- **Total 21,600** timed samples; **0 invalid**, **0 timeout**; every rep passed all gates.

**Direction-A per-arm latency (median across 3 reps, milliseconds):**

| case | arm | p50 | p90 | p99 | IQR |
|---|---|---|---|---|---|
| P0  | ray | 2.099 | 2.382 | 3.111 | 0.285 |
| P0  | hpx | 1.349 | 1.557 | 1.852 | 0.225 |
| P1  | ray | 113.546 | 114.001 | 115.011 | 0.507 |
| P1  | hpx | 114.189 | 116.385 | 127.390 | 1.728 |
| P2  | ray | 57.543 | 58.164 | 59.223 | 0.567 |
| P2  | hpx | 34.925 | 35.311 | 43.265 | 0.411 |
| P3a | ray | 6.881 | 7.153 | 7.494 | 0.296 |
| P3a | hpx | 6.179 | 6.427 | 16.653 | 0.272 |
| P3b | ray | 7.208 | 7.448 | 7.745 | 0.266 |
| P3b | hpx | 6.405 | 6.700 | 16.543 | 0.236 |
| P3c | ray | 11.034 | 11.315 | 11.628 | 0.270 |
| P3c | hpx | 8.391 | 8.719 | 14.369 | 0.236 |

Per-arm distributions only. These are plane-labeled measured facts, not a cross-arm
comparison.

**Completed read-only QD1 ratio review (narrow summary).** A separate, read-only review
examined per-case QD1 ratios and concluded:

- P0, P2, P3a, P3b, P3c were **conditionally licensed** for scoped per-case discussion.
- **P1 is per-arm-only** (not licensed for a ratio).
- The **repetition-level range is the primary uncertainty**.
- The accepted QD1 aggregates were **not modified**; their ratio fences remain false.

No broad comparative language is drawn from Slice 1.

---

## 6. Slice 2 methodology and results (bounded-concurrency throughput)

**Design:** coordinator = actor A; cases P0/P2/P3b; concurrency `C ∈ {2, 4}`;
`num_cpus = 2`, `hpx_threads = 2` (both arms, every case); N=1000 measured completions per
batch; warmup 100; R=3 reps; **fixed-count closed-loop** scheduler (a fixed number of
in-flight requests replaced on completion); **bounded dedicated verifier** (capacity 256)
running outside the measured request intervals; per-batch mechanism witnesses;
per-repetition P3b/C=4 concurrency-capability soak.

**Verified-completion goodput definition:** timing covers submission through caller-observed
completion (t1 after `ray.get`); correctness is established by the bounded verifier before
the batch is accepted, outside the timed intervals.

**Correctness / verifier totals (accepted):**

- **36,000** verified completions (3 cases × 2 C × 2 arms × 3 reps × 1,000), **0 invalid**.
- Verifier high-water **max 1** of 256; **enqueue-block 0 ns** (no verifier backpressure).
- Outstanding at target concurrency: fraction-at-C ≈ 0.999 (C=2), ≈ 0.997 (C=4).
- P3b/C=4 concurrency-capability **proven for both arms in every rep** (soak witness).
- All reps passed all island gates.

**Per-arm results (median across 3 reps; goodput/s, latency ms):**

| C | case | arm | goodput/s | p50 | p90 | p99 | IQR | coord_active_max | ray_peer_active_max | hpx_action_active_max |
|---|---|---|---|---|---|---|---|---|---|---|
| 2 | P0  | ray | 838.9  | 2.283 | 2.893 | 3.595 | 0.642 | 2 | 2 | 0 |
| 2 | P0  | hpx | 1417.4 | 1.325 | 1.562 | 2.266 | 0.206 | 2 | 0 | 1 |
| 2 | P2  | ray | 29.9   | 66.622 | 67.193 | 72.226 | 0.497 | 2 | 2 | 0 |
| 2 | P2  | hpx | 38.2   | 53.114 | 55.084 | 65.547 | 3.947 | 2 | 0 | 2 |
| 2 | P3b | ray | 263.1  | 7.625 | 8.181 | 8.683 | 0.549 | 2 | 2 | 0 |
| 2 | P3b | hpx | 275.9  | 7.411 | 8.091 | 10.160 | 1.221 | 2 | 0 | 2 |
| 4 | P0  | ray | 1316.7 | 3.008 | 3.976 | 4.917 | 1.187 | 4 | 3 | 0 |
| 4 | P0  | hpx | 2098.6 | 1.792 | 2.345 | 3.437 | 0.504 | 4 | 0 | 1 |
| 4 | P2  | ray | 33.1   | 124.636 | 155.634 | 183.123 | 39.254 | 4 | 4 | 0 |
| 4 | P2  | hpx | 53.7   | 71.950 | 95.095 | 113.286 | 19.283 | 4 | 0 | 2 |
| 4 | P3b | ray | 512.1  | 7.583 | 8.926 | 9.893 | 1.136 | 4 | 4 | 0 |
| 4 | P3b | hpx | 454.4  | 8.519 | 11.013 | 13.083 | 2.457 | 4 | 0 | 2 |

Note the mechanism witness at C=4/P3b: the Ray peer reached `active_max = 4`, while the
HPX peer action `active_max` was pinned at **2** (two HPX workers). This is the signature
that Slice 3 dissects.

**Completed read-only Slice 2 ratio review.** A separate, read-only review computed scoped
ratios. These were **not** written into the accepted aggregate (its fences remain false);
they are recorded here as the review's licensed, gated, scoped results, with mandatory
caveats.

*Licensed:*

- **P2 / C=2:** HPX/Ray goodput ratio ≈ **1.28** (repetition range 1.27–1.29);
  Ray/HPX p50-latency ratio ≈ **1.25** (range 1.25–1.26).
- **P0 / C=2:** goodput ratio ≈ **1.69** (range 1.60–1.77);
  p50-latency ratio ≈ **1.72** (range 1.60–1.88). **Fixed-overhead caveat mandatory** —
  P0 is a fixed-overhead control (V=64), so this reflects boundary/dispatch overhead, not
  a serving-throughput claim.

*Not licensed:*

- **P3b / C=2:** direction unstable (goodput ≈ parity, repetition-level sign not robust).
- **All original C=4 ratios:** resource-sensitive (the `C=4`, `num_cpus=2`, `hpx_threads=2`
  configuration oversubscribes the HPX peer; see §7–§11).

These scoped ratios are for exp69 internal interpretation only and do not license any
general Ray-vs-HPX claim.

---

## 7. Slice 3 static audit (thread-supply asymmetry, found before measurement)

A read-only implementation audit (Phase A) established the suspected asymmetry before any
new measurement:

- Ray actor **method-thread execution scales with `max_concurrency = C`**; at C=4 up to
  four Ray actor method threads can run native peer work concurrently.
- The native peer work **releases the GIL**, so those method threads run in parallel.
- Ray transport uses **separate runtime I/O threads**.
- The accepted Slice 2 HPX configuration supplied **only two HPX workers**.
- Those two HPX workers had to service, together: posted coordinator bodies, own-shard
  top-k, incoming peer actions, `.then` continuations, native merge, and suspended-task
  resumptions.

The **logical native work was matched** between arms (identical own top-k, peer top-k,
and merge). The **execution-thread supply was not** matched in the accepted Slice 2 band.

The audit found no crash-class defect, no GIL or Python mutex serializing the HPX timed
path, and confirmed both arms compute own-shard top-k before dispatching peer work.

Note: `num_cpus` is a Ray scheduling reservation, not a hard OS-thread cap. This document
does **not** claim `num_cpus` physically limits Ray OS threads; the band's `num_cpus` and
`hpx_threads` are matched together to keep the two arms in the same resource band.

---

## 8. Slice 3 matrix

Three resource bands, **P3b only** (the case that reversed in Slice 2), a fresh island
built per band per rep, both arms configured inside the same band:

```text
C2_cpu2_ht2   →  C=2, num_cpus=2, hpx_threads=2    (baseline_matched)
C4_cpu2_ht2   →  C=4, num_cpus=2, hpx_threads=2    (accepted_oversubscribed_control; = Slice 2 config)
C4_cpu4_ht4   →  C=4, num_cpus=4, hpx_threads=4    (matched_decisive)
```

Within a band, Ray and HPX arms receive the **same** `(C, num_cpus, hpx_threads)`;
resources are never given to only one arm.

Matrix totals (accepted, curated job 172125):

- 3 bands × R=3 × 2 arms = **18 batches**.
- **18,000** exact completions; **0 invalid**; **0 timeout**.
- No verifier backpressure; all `batch_pass` / `rep_pass` true; `island_gates_failed` empty
  for all 9 band-reps.
- Hard Ray placement on medusa[01,11,12]; distinct Ray node ids per role; subnet
  `10.42.5.`; work-free root; graceful lifecycle; **no Ray or peer orphans**.

---

## 9. Slice 3 endpoint results (per-arm, no cross-arm ratio)

Per-arm medians across 3 reps. All ratio/speedup/difference/winner fences are false for
Slice 3; no cross-arm ratio is computed.

**C2 / cpu2 / ht2 (baseline_matched):**

- Ray: goodput **263.9/s**; p50 **7.54 ms**; p90 **8.24 ms**; p99 **8.61 ms**.
- HPX: goodput **270.1/s**; p50 **7.24 ms**; p90 **8.12 ms**; p99 **18.10 ms**.

**C4 / cpu2 / ht2 (accepted_oversubscribed_control):**

- Ray: goodput **515.5/s**; p50 **7.59 ms**; p90 **8.52 ms**; p99 **9.82 ms**.
- HPX: goodput **444.1/s**; p50 **8.66 ms**; p90 **11.39 ms**; p99 **15.12 ms**.

**C4 / cpu4 / ht4 (matched_decisive):**

- Ray: goodput **514.1/s**; p50 **7.52 ms**; p90 **8.71 ms**; p99 **10.53 ms**.
- HPX: goodput **511.1/s**; p50 **7.58 ms**; p90 **9.74 ms**; p99 **12.00 ms**.

At the oversubscribed control band, the HPX-arm goodput distribution sat below the Ray-arm
distribution (the Slice 2 P3b/C=4 reversal, reproduced). At the matched decisive band,
**the two per-arm goodput distributions converged to the same approximate region**. This
is a distributional statement; it is not a claim that they were identical, and no
equivalence is asserted statistically.

---

## 10. Native decomposition (HPX coordinator-local and peer-local)

Exact measured HPX coordinator-local stage timings (median across reps of per-rep p50).
All timings are coordinator-local monotonic durations on a single controller clock; no
cross-node clock arithmetic is performed.

| stage | C2/2/2 | C4/2/2 | C4/4/4 |
|---|---|---|---|
| entry_to_post | 0.31 µs | 0.32 µs | 0.31 µs |
| post_to_start (posted-task queue delay) | 4.72 µs | 4.96 µs | 4.83 µs |
| own top-k | 2.252 ms | 2.254 ms | 2.247 ms |
| dispatch | 16.4 µs | 13.3 µs | 15.5 µs |
| **dispatch-to-continuation interval** | **2.800 ms** | **3.472 ms** | **2.453 ms** |
| reply-get | 0.49 µs | 0.38 µs | 0.41 µs |
| merge | 4.85 µs | 4.51 µs | 4.90 µs |
| continuation-to-delivery (fulfillment) | 2.72 µs | 2.07 µs | 2.83 µs |
| **total native coordinate** | **5.624 ms** | **6.927 ms** | **5.473 ms** |

> **Terminology.** The "dispatch-to-continuation interval" (field `dispatch_to_cont`) is a
> **composite** interval spanning from peer dispatch until the continuation becomes runnable
> or begins executing. It may include peer action scheduling, peer local top-k, HPX
> serialization and parcel transit, reply handling, and continuation readiness/scheduling.
> It is **not** pure continuation queueing and **not** pure transport time. Isolating
> queue-ready-to-continuation-start would require a separately measured timestamp that this
> run does not capture.

Peer-local decomposition (recorded on the peer node, **NOT subtracted** from coordinator
clocks; never differenced across node clocks):

| peer-local stage (median of per-rep p50) | C2/2/2 | C4/2/2 | C4/4/4 |
|---|---|---|---|
| peer local top-k | 2.244 ms | 2.246 ms | 2.247 ms |
| peer action body | 2.247 ms | 2.249 ms | 2.250 ms |

Invariance and the moving interval:

- **own top-k** stays ≈ **2.25 ms** across all bands (matched native work).
- **peer-local top-k** stays ≈ **2.24–2.25 ms** across all bands (matched native work).
- **merge** and **reply-get** stay essentially constant.
- the **composite dispatch-to-continuation interval rises** in C4/2/2 (2.80 → 3.47 ms) and
  **falls** in C4/4/4 (→ 2.45 ms, at or below the C2 baseline).
- **total native coordinate** follows the same pattern (5.62 → 6.93 → 5.47 ms).

Worker identities and peer concurrency:

| band | HPX workers observed | own-work workers | continuation workers | same-worker fraction | peer active max (per rep) |
|---|---|---|---|---|---|
| C2/2/2 | 2 | {0,1} | {0,1} | 0.676 | 2, 2, 2 |
| C4/2/2 | 2 | {0,1} | {0,1} | 0.503 | 2, 2, 2 |
| C4/4/4 | 4 | {0,1,2,3} | {0,1,2,3} | 0.245 | 4, 3, 3 |

Own-work and continuation draw from the **same** default HPX pool in every band
(`own workers == continuation workers`), so there is no separate continuation-pool that
is starved independently of total worker supply.

---

## 11. Causal interpretation

> The evidence supports a **thread-supply resource asymmetry** as the cause of the P3b/C=4
> reversal in the original `C4/cpu2/ht2` band.

Why:

1. The original reversal **reproduced** at `C4/cpu2/ht2` (HPX-arm goodput distribution
   below the Ray-arm distribution; HPX peer action `active_max` pinned at 2).
2. The **fixed compute stages were invariant** across bands (own top-k, peer-local top-k,
   merge, reply-get).
3. The **Ray-arm timings were flat** across all three bands (Ray's method-thread supply
   already scaled with `C`, so it was not starved).
4. Raising HPX workers to four (`C4/cpu4/ht4`) **raised observed peer concurrency**
   (peer active max 2 → 3–4).
5. The **load-sensitive composite dispatch-to-continuation interval fell** (3.47 → 2.45 ms)
   and **total native coordinate fell** (6.93 → 5.47 ms).
6. The **HPX-arm endpoint goodput distribution returned to the Ray-arm region**.

Classification:

```text
thread_supply_resource_asymmetry_supported
no_implementation_defect_observed
```

This causal statement is scoped to this exact experiment (P3b, this workload, this
topology, this HPX build, these bands). It does not establish universal causality and does
not license any general Ray-vs-HPX performance claim.

---

## 12. Metadata correction (job 172122 → 172125)

Job **172122** passed every **measurement** gate (18,000 verified, 0 invalid, 0 timeout,
`overall: pass`, top-level `cross_node: true`), but its aggregate **inherited two incorrect
claim-fence metadata values** from the QD1-shaped aggregate builder:

```text
not_cross_node = true                    # WRONG: this run IS cross-node
qd1_latency_only_no_throughput = true    # WRONG: this run IS throughput-shaped, not QD1-latency-only
```

The runner's claim-fence construction for the cross-node Slice 3 phase was corrected to
flip those two fences (`slice3_crossnode_claim_fences`), and job **172125** re-ran the
identical matrix to regenerate the final curated aggregate with correct metadata:

```text
not_cross_node = false
qd1_latency_only_no_throughput = false
```

Scope of the correction:

- **No measured code path, mechanism, workload, placement, resource band, or gate changed**
  between the two jobs; the change was to aggregate claim-fence metadata honesty.
- 172125 is an independent re-execution; its per-batch endpoint values differ from 172122
  only by ordinary run-to-run variation.
- **Both jobs independently reproduce the same result:** the reversal at `C4/cpu2/ht2` and
  the convergence at `C4/cpu4/ht4`, and the same causal classification.

The curated Slice 3 aggregate is the corrected 172125 output.

---

## 13. Evidence and reproducibility

**Curated aggregates (tracked; exact MD5 verified this session):**

| aggregate | slice / job | MD5 |
|---|---|---|
| `same_axis_topk_qd1_local_aggregate.json` | Slice 1 QD1 local | `6f0dba20beb08a038e506152739b6315` |
| `same_axis_topk_qd1_crossnode_aggregate.json` | Slice 1 QD1 cross-node (171408) | `cb9a990c3e7d9dfab453bcd038618b41` |
| `same_axis_topk_throughput_crossnode_aggregate.json` | Slice 2 throughput (171561) | `be5ff54faa4fce76ee11b7baaeee948f` |
| `same_axis_topk_slice3_crossnode_aggregate.json` | Slice 3 causal (172125) | `3ad8ba1432ecef22a5c967da78aadd8c` |

**Raw evidence (gitignored):** per-job run directories under
`experiments/69_same_axis_topk_perf/_exp69_runs/` (including
`crossnode_accepted_171408_*`, `slice3_smoke_172121_*`, `slice3_accepted_172122_*`,
`slice3_accepted_172125_*`), their `head.log` / `worker_*.log` Slurm outputs, per-rep
`samples.jsonl`, and scratch smoke aggregates. These are not tracked and are not curated
evidence.

---

## 14. Safe conclusions

> exp69 establishes exact, matched-boundary latency (Slice 1) and bounded-concurrency
> performance distributions (Slice 2) for two peer-orchestration paths inside the same
> Ray-hosted, HPX-resident topology, with every measured sample verified bit-exactly.

> The original P3b/C=4 reversal was specific to a `C=4` workload driven through a
> two-worker HPX runtime; matched `C=4` worker supply (`cpu4/ht4`) removed the observed
> thread-supply bottleneck, and the two per-arm goodput distributions converged to the
> same approximate region.

Slice 2 remains valid **as measured** for its exact `C4/cpu2/ht2` configuration; Slice 3
confirms that Slice 2's "resource-saturation-sensitive" caveat on the C=4 result was the
correct reading. Phases D–F (dispatch-first, continuation-placement, worker-blocking
variants) are **not needed**: the matched-resource matrix resolved the cause.

---

## 15. Explicit non-claims

- Not real inference; the workload is synthetic, LLM-*shaped* only.
- No model weights, no tokenizer, no GPU.
- No standalone Ray-versus-HPX conclusion (both arms share the same Ray-hosted,
  HPX-resident topology; the Ray arm is not a pure standalone-Ray deployment).
- No universal winner and no broad speedup claim.
- No pooled ratio; no cross-arm ratio, difference, or winner is computed for Slice 3.
- No claim that HPX always performs better; no claim that Ray is generally slower.
- No claim that an active-max of 1 proves serialization.
- No claim that `num_cpus` physically caps Ray OS threads.
- No claim that the dispatch-to-continuation interval is pure transport or pure
  continuation-queue time (it is a composite; see §10).

---

## 16. Future guard (comparison validity)

To keep future same-axis bands honest for `C ≥ 4`:

```text
For C >= 4, require num_cpus == hpx_threads == C,
or label the band explicitly as oversubscribed.
```

This is a **comparison-validity guard** for the exp69 measurement design — it ensures both
arms enter a band with matched execution-thread supply so a measured difference is not a
resource-supply artifact. It is **not** a general HPX runtime requirement and says nothing
about how HPX should be configured outside this comparison.
