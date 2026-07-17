# exp68 — Deterministic vocabulary-sharded top-k across two Ray-hosted HPX localities

Durable experiment-local account of exp68. This is the standing record of the workload,
mechanism, correctness contract, cross-node evidence, and claim boundaries.

exp68 is an **architectural and correctness gate**, not a performance comparison. All
numbers, job IDs, hashes, node IDs, and source names below were verified against the two
curated aggregates and the copied-back Rostam evidence in this directory.

Directory: `experiments/68_vocab_sharded_topk/`.

---

## 1. Executive summary

The question exp68 answers:

> Can two Ray-hosted HPX localities execute a deterministic vocabulary-sharded top-k
> request across nodes, exchange shard candidates through an HPX action, merge them
> exactly, and prove topology, mechanism, correctness, and clean lifecycle?

Answer, on the tested environments: **yes**. Two Ray actor workers on distinct nodes each
host an in-process HPX connect-mode locality; each owns a disjoint vocabulary shard. Each
coordinator computes its own shard's local top-k, dispatches an HPX action to the **peer**
locality to fetch that shard's local top-k, and merges the two candidate lists through an
HPX future continuation. The global top-k, and each shard's local top-k, match an
independent Python oracle **bit-exactly** in token IDs, ordering, and float32 bit patterns,
in **both** coordinator directions, across a fixed seven-case matrix, with a work-free HPX
root, hard placement, clean lifecycle, and no orphans.

exp68 is a functional/architectural/correctness gate. It computes **no** Ray-vs-HPX
performance comparison and licenses no speedup, ratio, or winner claim.

---

## 2. Relationship to exp66 and exp67

Verified from the exp66/exp67 curated aggregates in this repository:

- **exp66** (`hpx_connect_locality_hosted_in_one_ray_actor_worker`): **one** Ray actor
  worker hosts **one** networking HPX connect-mode locality **in-process** (`hpx::start`,
  PID identity, no HPX child process), executes verified remote HPX actions, and makes
  progress. Passed locally and across **two** Rostam nodes (medusa00 → medusa01).
- **exp67** (`two_ray_actors_share_one_hpx_runtime_actor_to_actor`): **two** distinct Ray
  actor workers each host an in-process HPX locality and join **one shared** HPX runtime
  under a separately supervised work-free root; **bidirectional** actor-to-actor HPX
  actions prove each actor's PID/locality/hostname remotely. Passed locally and across
  **three** Rostam nodes (root medusa00, actor A medusa01, actor B medusa11).
- **exp68** (this experiment): adds **useful deterministic distributed computation** across
  the exp67 two-actor shared-runtime topology — a vocabulary-sharded top-k reduction with
  an independent bit-exact oracle.

Progression: exp66 proves in-process hosting; exp67 proves two actors in one shared runtime
communicating both ways; exp68 proves an exactly checkable distributed operation over that
runtime. exp68 does not restate or strengthen the exp66/exp67 hosting proofs; it depends on
them and adds the workload-and-oracle layer.

---

## 3. System topology

**Cross-node accepted (Slurm job 170746):**

| role | node | selected IP | HPX locality | process (rep 1 witness) |
|---|---|---|---|---|
| root / controller (work-free) | medusa00 | 10.42.5.30 | 0 | pid 2487871 |
| actor A / HPX locality | medusa01 | 10.42.5.31 | 1 | pid 1367833 |
| actor B / HPX locality | medusa11 | 10.42.5.41 | 2 | pid 3750322 |

- Slurm nodelist `medusa[00-01,11]`; allocation nodes `medusa00, medusa01, medusa11`.
- Subnet prefix `10.42.5.` (the `eno16` / `10.42.5.x` interfaces).
- Distinct Ray node IDs: nodeR `7a5e03b70e…`, nodeA `e05c17336e…`, nodeB `6956ef341b…`;
  driver on nodeR (`driver_on_nodeR: true`, driver_hostname `medusa00.rostam.cct.lsu.edu`).
- Transport `tcp_cross_node` (HPX TCP parcelport, per-node `10.42.5.x` endpoints).
- Hard Ray placement (each actor pinned to its node); Ray node ID treated as authoritative.
- Per-actor config: `hpx_threads = 2` (default), `ray_num_cpus = 2` (default).
- Runtime: HPX commit `20bc3d4bf3068383edcb63be13f22e9ff95842fa` (HPX V2.0.0, AGAS V3.0,
  Boost 1.91.0, Hwloc 2.12.0; release build dated Jul 13 2026, GCC 15.1.0); Ray 2.55.1
  (commit `237c2455…`); Python 3.12.3 (CPython, GIL-enabled, non-free-threaded).
- Accepted cross-node job: **170746** (3 reps). Cross-node smoke: **170745** (1 rep,
  scratch aggregate).

**Local control:**

- Single host (macOS, `Bitas-MacBook-Air.local`, arm64, 10 cores); root + actor A + actor B
  all in one host over `tcp_loopback`; `single_node = true`.
- Runtime: HPX commit `20bc3d4bf3…`; Ray 2.55.1; **Python 3.11.15** (CPython, GIL build).
- No Slurm job (Mac-local).

The local and cross-node phases ran on **different Python builds** (3.11.15 vs 3.12.3),
consistent with the design's interpreter-independent hosting property; the deterministic
values and oracle agreement held on both.

---

## 4. Workload definition

LLM-**shaped** synthetic next-token candidate selection (not real inference; see §16).

- **Vocabulary:** token IDs `0 … V-1`. **Shard split:** actor A owns `[0, split)`, actor B
  owns `[split, V)` — disjoint and complete (`partition_ok: 0 < split < V`).
- **Deterministic logit** for `(token_id, seed)` (rule `int_grid_over_8`, float32-exact):

  ```text
  h    = (uint32(token_id) * 2654435761 + uint32(seed) * 40503) mod 2^32
  grid = (h % 131) - 65                 # integer grid in [-65, 65]
  logit = float32(grid) / 8.0           # exact float32; value range [-8.125, 8.125]
  ```

- **Token-ID scheme:** global integer IDs; each shard computes over its half-open range.
- **Candidate payload:** a `(token_id, logit_bits)` pair — the raw IEEE-754 float32 **bit
  pattern**, so all comparison is on exact bits, not decimal.
- **Total order** (used by per-shard top-k, the native merge, and the oracle): higher logit
  wins; on equal logit, lower **global** token ID wins (stable descending order).
- **Per-shard top-k:** the `k` best tokens over a shard by the total order (`stable_sort`).
- **Global merge:** union the two local top-k lists and take the global top-k by the same
  total order. Each shard's local top-k of size `k` is a correct superset contributor, so
  the union is a correct superset for the global top-k.

**Fixed seven-case matrix** (not a sweep). Σk = 34.

| case | V | split | k | seed | coverage purpose |
|---|---|---|---|---|---|
| tiny_k1 | 8 | 4 | 1 | 7 | small, k=1, k < shard |
| tiny_k3 | 8 | 4 | 3 | 7 | small, k>1, k < shard |
| cross_both | 64 | 32 | 6 | 1 | winners from both shards |
| tie_cutoff | 200 | 100 | 5 | 1 | tie exactly at the cutoff |
| shardA_dom | 128 | 64 | 8 | 1 | one shard dominant |
| both_contrib | 100 | 50 | 10 | 1 | both shards contribute |
| k1_large | 256 | 128 | 1 | 13 | k=1 over a larger vocab |

The seeds were chosen (and asserted in the selftest) to actually exhibit the declared
coverage (e.g., a genuine tie at the cutoff, one-shard-dominant, both-shards-contribute).

---

## 5. Exact execution path

For each case, in **both** coordinator directions:

1. The Python **controller** invokes the coordinator actor's `coordinate(...)`.
2. On an HPX thread (`hpx::run_as_hpx_thread`), the coordinator computes its **own shard's**
   local top-k in-process and stamps its own pid/locality/host.
3. It locates the **peer** locality by matching `peer_loc` against `find_all_localities()`
   and dispatches `hpx::async<exp68_local_topk_action>(peer, peer_lo, peer_hi, seed, k)`.
4. The **peer** computes its shard's local top-k and returns it plus the peer process's
   identity witnesses.
5. The reply (serialized candidates + peer pid/locality/host) returns over the HPX
   parcelport.
6. The coordinator **merges** own + peer candidates natively inside a `.then` **future
   continuation** that runs on an HPX worker when the fetch future is ready.
7. The controller receives the global top-k, both local top-k lists, both identity witness
   sets, and HPX-composition evidence flags.
8. The **independent Python oracle** verifies the result bit-exactly after delivery.

**Peer action is asynchronous and continuation-composed** (from source): `hpx::async` →
`future::then` continuation performing the merge → `merged.wait_for(bound_s)` (bounded;
returns not-ready rather than hanging) → `merged.get()`. Ray never carries the peer shard
or the peer's local top-k — the two actors hold **no Ray handle to each other**, so the
peer path is HPX-only by construction.

---

## 6. HPX action and serialization

- **Registered actions** (`topk_action.hpp`, registered in exactly one TU per binary — the
  exp63/64/66/67 registration discipline):
  - `exp68_local_topk_action` ← `exp68_local_topk(lo, hi, seed, k)` → `exp68_topk_reply`.
  - `exp68_pid_action` ← `exp68_pid()` → `int64` (identity probe).
- **Request arguments:** `(lo, hi, seed, k)` — the peer's half-open shard, the seed, and k.
- **Reply structure** `exp68_topk_reply`:

  ```cpp
  struct exp68_topk_reply {
      std::vector<exp68::Cand> cands;   // Cand = std::pair<int64 token_id, uint32 logit_bits>
      std::int64_t  pid;                // executing peer process pid
      std::uint32_t locality;           // executing peer HPX locality id
      std::string   host;               // executing peer hostname
      template<class Ar> void serialize(Ar& ar, unsigned){ ar & cands & pid & locality & host; }
  };
  ```

- **Serialization:** HPX **intrusive** serialization; `std::vector<std::pair<int64,uint32>>`
  plus scalars and `std::string` are HPX built-ins. Candidates travel as raw
  `(token_id, float32-bit-pattern)` pairs, so bit preservation is checkable end-to-end.
- **Identity witnesses in the reply:** peer pid, peer HPX locality id, and peer hostname —
  returned **by value** so the coordinator proves which process/locality computed the peer
  shard.
- **Proof the HPX path was actually used:** per-call composition flags
  `action_dispatched`, `future_ready`, `continuation_executed`, `result_delivered` (all
  required true); plus `a_peer_is_b` / `b_peer_is_a` identity gates (peer locality **and**
  peer pid must equal the expected remote actor); plus `operation_over_hpx_not_ray` (the
  actors hold no mutual Ray handle for the application path). Direct wire/serialization
  instrumentation is **not** claimed (`no_direct_serialization_or_wire_claim`).

---

## 7. Correctness oracle

The oracle in `run_exp68.py` is an **independent** stdlib reimplementation (Python `struct`
for exact float32 packing) that mirrors `shared_topk.hpp` bit-for-bit. It recomputes, from
scratch, each shard's top-k and the global top-k, and compares against the actors' results.
Because it re-derives the grid rule, the total order, the shard boundaries, and the merge
independently, it detects:

- **incorrect shard boundaries** — via `local_topk_a_correct` / `local_topk_b_correct`
  (each shard's local top-k must equal `oracle_topk(shard)`) and `shard_partition_ok`.
- **incorrect token offsets** — token IDs are global; wrong offsets change the ID set.
- **wrong ordering** — `local_topk_order_ok` (`is_sorted_total_order`) and
  `global_token_ids_exact`.
- **tie-breaking errors** — the `tie_cutoff` case plus the lower-global-ID tie rule in the
  oracle and `is_sorted_total_order`.
- **float conversion errors** — `float32_bits_exact` (bit patterns, not decimals).
- **incomplete candidate sets** — a missing/short candidate list breaks
  `a_merge_equals_oracle` / `b_merge_equals_oracle`.
- **wrong peer locality** — `a_peer_is_b` / `b_peer_is_a` (locality **and** pid),
  `no_application_on_root`.
- **wrong merge result** — `a_merge_equals_oracle` / `b_merge_equals_oracle` and
  `coordinator_symmetry` (both directions must agree).

**Gate families** (verdict rule: PASS iff Slice A, Slice F, and every matrix case's
B/C/D/E gates — including cross-node bit preservation — pass in **every** rep, with no
orphans on any node):

- **Slice A** — topology + shard ownership (20 gates): fixed HPX commit, both actors
  started in-worker, two distinct Ray processes, distinct nonzero localities, no HPX child,
  shared-island membership ≥ 3, root isolated/work-free (locality 0), all shards
  disjoint/complete, evidence complete.
- **Slice B/C/D/E** — per case (both directions): local top-k correctness, A-coordinates,
  B-coordinates, coordinator symmetry, exact global IDs, float32 bits, HPX composition,
  no application on root.
- **Slice F** — lifecycle (12 gates): graceful leave, clean HPX stop, root finalized clean,
  heartbeat completion, actor A/B recreation, no orphans, all waits bounded,
  `ray_no_actor_payload_path`.
- **Slice G** — saturation diagnostic, **non-gating** (`affects_verdict: false`).

**Accepted totals (both phases): 3 reps × 7 cases = 21 case-evaluations each; every Slice A
(20), Slice F (12), and per-case gate True; every `rep_pass` True; every `failure_class` =
`pass`.** The failure taxonomy has 20 named classes (only `pass` was observed).

---

## 8. Local-control evidence

- **Purpose:** prove the full mechanism and oracle on a single host before committing a
  Slurm allocation; interpreter-independent-hosting control on a different Python build.
- **Topology:** root + actor A + actor B co-resident on one macOS host over `tcp_loopback`;
  distinct nonzero HPX localities; membership ≥ 3.
- **Sampling:** 3 reps, the full 7-case matrix, both coordinator directions.
- **Correctness:** 21 case-evaluations, all gates True, all reps pass; 204 own-shard local
  top-k candidates computed (Σk=34 × 2 shards × 3 reps).
- **Lifecycle:** graceful leave, clean stop, root finalize, actor recreation, no orphans.
- **Artifacts:** curated `vocab_sharded_topk_aggregate.json`; raw runs under
  `_exp68_runs/2026071*T*Z/` (gitignored).
- **What it proves:** the mechanism and the oracle are correct and deterministic, and two
  in-process HPX localities under Ray actors compute the exact sharded top-k.
- **What it does not prove:** cross-node placement, real network transport, or distinct
  physical hosts (all localities share one host over loopback).

---

## 9. Cross-node accepted evidence (job 170746)

| item | value |
|---|---|
| accepted Slurm job | 170746 (cross-node smoke: 170745, 1 rep) |
| allocation nodes | medusa00 (root), medusa01 (actor A), medusa11 (actor B) |
| reps | 3 |
| cases per rep | 7 (both coordinator directions) |
| case-evaluations | 21 |
| verified (all gates True) | 21 / 21 |
| invalid | 0 |
| timeout / not-ready | 0 |
| transferred peer candidates over HPX action | **204** (Σk 34 × 2 directions × 3 reps) |
| placement verdict | hard placement, three roles on three distinct nodes on 10.42.5.x — PASS |
| HPX identity verdict | distinct nonzero localities (A=1, B=2), root=0 — PASS |
| mechanism verdict | HPX composition flags all true; `operation_over_hpx_not_ray` — PASS |
| lifecycle verdict | graceful leave, root finalize (membership 3→1, `leave_observed`), actor recreation — PASS |
| orphan verdict | `no_ray_orphans` and `no_peer_orphans` on nodeR/A/B — PASS |
| overall | **pass** |

Transferred-candidate arithmetic: per case the peer returns `k` candidates; Σk over the
seven cases = 1+3+6+5+8+10+1 = 34. Both coordinator directions per rep → 2 × 34 = 68; over
3 reps → **204** peer-candidate `(token_id, float32-bits)` pairs crossed the HPX action
path. Every case-evaluation's native merge reproduced the independent global oracle
bit-exactly in both directions — zero mismatches in token IDs, ordering, or float32 bits.

---

## 10. Placement and identity proof

exp68 proves the intended cross-node path by value, not by assumption:

- **A and B on distinct Ray nodes** — distinct Ray node IDs (nodeA `e05c17336e…`,
  nodeB `6956ef341b…`), hard placement; distinct actor PIDs (A 1367833, B 3750322).
- **Root separate and work-free** — root on nodeR (`7a5e03b70e…`), HPX locality 0, pid
  2487871; `no_application_on_root` (every merge/own/peer locality is a nonzero actor
  locality); the root binary hosts only heartbeat/membership introspection.
- **HPX localities map to the expected actor processes** — the peer reply carries pid +
  locality + hostname; `a_peer_is_b` requires peer_locality == B's locality **and**
  peer_pid == B's pid (and symmetrically `b_peer_is_a`).
- **Selected interfaces on the intended subnet** — root/A/B IPs 10.42.5.30 / .31 / .41,
  subnet prefix `10.42.5.`.
- **Peer action executes on the intended remote locality** — the returned hostname
  (`medusa01…`, `medusa11…`), pid, and locality agree with the placed actor.
- **No fallback to loopback / same-host** — actor hostnames differ (medusa01 vs medusa11),
  localities differ (1 vs 2), PIDs differ; transport is `tcp_cross_node`.

---

## 11. Lifecycle and failure containment

Per rep, exp68 exercises and gates the **clean-path** lifecycle:

- **Actor creation** — two Ray actors created, hard-placed.
- **HPX startup** — each actor `hpx::start` in-process (connect mode), no HPX child process
  (`both_in_process_no_hpx_child`).
- **Membership** — the work-free root plus both actors reach membership 3
  (`shared_island_membership`, root `max_membership ≥ 3`). Admission here is the fixed
  three-locality island for the request (not late/elastic admission).
- **Peer readiness** — coordinator locates the peer via `find_all_localities()` before
  dispatch.
- **Graceful leave** — each actor `post(hpx::disconnect)` + `hpx::stop`, `stop_rc == 0`.
- **Root finalization** — root waits (bounded) for connectors to leave, observes membership
  back to 1 (`leave_observed: true`, `final_membership: 1`), then `hpx::finalize`
  (`root_finalized_clean`).
- **Actor recreation** — actors destroyed and recreated with new PIDs
  (`actor_a_recreation` / `actor_b_recreation`).
- **Ray worker teardown / orphan scans** — Ray head/worker cleanup on all three nodes;
  `no_ray_orphans` and `no_peer_orphans` on nodeR/A/B.

exp68 tests **clean-path lifecycle only**. It does **not** test ungraceful failure,
crash-recovery, eviction, or membership churn (`no_elasticity_churn_or_recovery`).

---

## 12. Results

Accepted functional results (both phases):

- **Cases completed:** 7-case matrix, both coordinator directions, every rep.
- **Repetitions:** local 3, cross-node 3.
- **Correctness:** 21 case-evaluations per phase, all gates True; 0 invalid, 0 timeout;
  cross-node 204 transferred peer candidates, zero mismatches; global and per-shard top-k
  bit-exact against the independent oracle in both directions.
- **Topology proof:** distinct nodes/PIDs/localities, work-free root, correct subnet
  (cross-node); membership ≥ 3.
- **Mechanism proof:** HPX composition flags all true; identity witnesses agree;
  `operation_over_hpx_not_ray`.
- **Lifecycle proof:** graceful leave, clean root finalize, actor recreation, no orphans.

**Timings.** exp68 collected **no comparison-licensed timings**. The only timing-adjacent
observation is the **non-gating** Slice G saturation diagnostic (case `cross_both`, all 3
reps): `overlap_observed: true`, `progressed_under_dest_saturation: true`,
`affects_verdict: false`. It witnesses that the destination locality kept making progress
under overlap; it is explicitly diagnostic, not a latency/throughput/performance result,
and is fenced by `saturation_is_diagnostic_not_verdict` and `timing_not_correctness_oracle`.

---

## 13. Engineering findings

Recovered from source, headers, and prior-experiment discipline:

- **Action registration discipline:** the fixed actions live in `topk_action.hpp`, included
  in **exactly one** translation unit per binary (`actor_ext.cpp` and `exp68_peer.cpp`), so
  every island binary registers an identical action table (the exp63/64/66/67 rule).
- **Serialization-safe candidate representation:** candidates as
  `std::pair<int64 token_id, uint32 logit_bits>` ride HPX built-in serialization and keep
  IEEE-754 bit exactness across the wire; the reply co-carries identity witnesses.
- **Continuation-composed merge:** the merge runs inside a `.then` continuation on an HPX
  worker when the fetch future is ready, with a **bounded** `wait_for` so a stalled peer
  yields a not-ready result rather than a hang.
- **Root isolation:** the standalone `exp68_peer --role root` is console-mode (AGAS root /
  locality 0), heartbeats/observes membership only, and finalizes on `root.done` **after**
  a bounded wait for connectors to leave — so finalize never races an actor's
  `post(disconnect)+stop`.
- **In-process hosting:** `hpx::start` (connect mode) with GIL released; `no HPX child`
  gate confirms same-process hosting.
- **Endpoint selection / localhost:** connect mode maps `"localhost"` → `127.0.0.1` with no
  resolver (exp66/67 finding); cross-node uses the selected `10.42.5.x` endpoints.
- **Deterministic tie handling:** the `1/8` power-of-two grid makes float32 exact (no
  rounding), and the lower-global-token-ID tie rule is shared by C++ and the oracle.
- **Interpreter independence:** the mechanism passed on both Python 3.11.15 (local) and
  3.12.3 (cross-node) GIL builds.
- **Ray actor concurrency:** default `num_cpus = 2` per actor, `hpx_threads = 2`; exp68 does
  not stress concurrency (that is exp69's axis).

---

## 14. Evidence and reproducibility

**Tracked (committed) — the curated evidence:**

| artifact | phase | live MD5 |
|---|---|---|
| `vocab_sharded_topk_aggregate.json` | local control | `839683a07081fec628b98f6fb23be5ef` |
| `vocab_sharded_topk_crossnode_aggregate.json` | cross-node accepted (170746) | `d594f2a9b6fdae4c4410cd82b71ea8f1` |

Also tracked: `run_exp68.py`, `actor_ext.cpp`, `exp68_peer.cpp`, `topk_action.hpp`,
`shared_topk.hpp`, `CMakeLists.txt`, `exp68_crossnode.sbatch`, `.gitignore`.

**Gitignored (raw evidence under `_exp68_runs/`):** per-rep marker/sample directories
(`rep_1/…`, `rep_2/…`, `rep_3/…`), the local raw runs (`2026071*T*Z/`), the cross-node run
dirs (`crossnode_170745_*`, `crossnode_170746_*`) with `head.log` / `worker_a.log` /
`worker_b.log` / root logs, the Slurm outputs (`exp68xn_170745.out`, `exp68xn_170746.out`),
and the scratch smoke aggregate (`smoke_crossnode_agg.json`, job 170745). Build outputs
(`build/`, `*.so`, `*.o`, `__pycache__/`) are also ignored.

---

## 15. Safe conclusions

> exp68 demonstrates that two hard-placed Ray actors can host HPX localities in one shared
> distributed runtime and execute an exact deterministic vocabulary-sharded top-k operation
> through an HPX peer action.

> Every accepted result matched the independent oracle exactly in token IDs, ordering, and
> float32 bits, while process, locality, host, and lifecycle witnesses proved the intended
> cross-node execution path.

Scope: the tested local (macOS, Python 3.11.15) and three-node Rostam (Python 3.12.3)
environments, the fixed seven-case matrix, HPX commit `20bc3d4bf3…`, Ray 2.55.1.

---

## 16. Explicit non-claims

- Not real model inference; the workload is LLM-**shaped** synthetic candidate selection.
- No tokenizer, no model weights, no GPU, no training.
- No standalone Ray-versus-HPX comparison (both arms run inside the same Ray-hosted,
  HPX-resident topology).
- No speedup, ratio, or winner claim; exp68 collected no comparison-licensed timings.
- No general production-scalability claim.
- No ungraceful-failure or recovery claim (only clean-path lifecycle tested).
- No dynamic-membership or elasticity conclusion beyond the exact fixed three-locality
  topology used.
- No performance conclusion from the diagnostic saturation-progress witness
  (`affects_verdict: false`).
- No direct wire/serialization-format claim (`no_direct_serialization_or_wire_claim`); the
  HPX path is proven by identity witnesses and composition flags, not by wire inspection.

---

## 17. Relationship to exp69

exp69 **reused** exp68's deterministic vocabulary-sharded top-k workload and correctness
contract — the same `int_grid_over_8` generation rule, the same total order, the same
independent bit-exact oracle, and the same two-Ray-actor HPX-resident topology — to conduct
a **matched-boundary performance study** at the Python caller boundary.

- **exp68** proves the distributed operation and the oracle (this document).
- **exp69** adds a controlled **Ray-mediated versus HPX-mediated** peer-path
  characterization for the identical workload under strict same-axis gates (Slices 1–3),
  including a resource-band causal decomposition.
- exp69 **does not replace** exp68's architectural/correctness evidence; it builds on it.
  exp68 remains the foundation that guarantees any exp69 timing sample is verified
  bit-exactly before it counts.

---

## 18. Remaining limitations

Supported by the actual design:

- Small fixed topology (one work-free root + two actor localities; three nodes cross-node).
- Synthetic deterministic workload (candidate top-k, not a full inference pipeline).
- CPU-only; no real model, tokenizer, or GPU.
- Limited concurrency (QD1-style per-request coordination; concurrency is exp69's axis).
- Clean-path focus (no ungraceful failure, recovery, eviction, or churn).
- Fixed cluster and software stack (Rostam medusa nodes; HPX `20bc3d4bf3…`; Ray 2.55.1;
  Python 3.11.15 local / 3.12.3 cross-node).
- Fixed seven-case matrix rather than a broad parameter sweep.
