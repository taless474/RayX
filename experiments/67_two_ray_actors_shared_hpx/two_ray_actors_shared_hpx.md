# exp67 — Two Ray actors sharing one HPX runtime, with bidirectional actor-to-actor HPX actions

Durable experiment-local account of exp67. This is the standing record of the shared-runtime
architecture, the bidirectional action mechanism, the correctness/lifecycle gates, and the
claim boundaries. exp67 is an **architectural and mechanism gate**, not a performance
comparison.

Directory: `experiments/67_two_ray_actors_shared_hpx/`.

---

## 1. Executive summary

The question exp67 answers:

> Can **two distinct Ray actor workers**, each hosting a networking HPX locality in-process
> (the exp66 mechanism), join **one shared HPX runtime** under a separately supervised,
> work-free root — and execute HPX actions **from one actor's locality to the other's, in
> both directions** — with clean lifecycle and no orphans?

**Verdict: yes**, in the tested local and cross-node configurations (3/3 passing repetitions
each, all gates green). Two Ray actors (A and B) each import a native extension and call
`hpx::start` in `runtime_mode::connect`; both localities join a single HPX island whose
membership reaches **three localities** (work-free root = locality 0, actor A = locality 1,
actor B = locality 2). Actor A then dispatches HPX actions **at** actor B's locality and vice
versa; each direction returns the destination's PID, a closed-oracle value that encodes the
executing locality, and a remotely-reported hostname.

The pivotal architectural property is proven **by construction**: actors A and B hold **no
Ray handle to each other**. The only channel between their localities is the HPX parcelport
— so the actor-to-actor operation path is HPX, not Ray, while Ray retains placement,
process lifecycle, supervision, and actor recreation.

Cross-node, the three roles ran on **three distinct nodes** with hard Ray placement, and both
action directions genuinely crossed nodes (destination PID, locality, and FQDN hostname all
witnessed remotely). Scope: a single actor pair, a synthetic closed-`int64` probe, and a
clean-path lifecycle only.

---

## 2. Motivation

exp66 proved that *one* Ray actor worker can host a networking HPX locality in-process. That
alone does not give a usable hybrid architecture: the interesting design — Ray owns
placement/lifecycle, HPX owns the distributed operation path — requires **multiple** Ray-hosted
localities that are members of **one** HPX runtime and can address each other directly with
HPX actions.

exp67 is the smallest experiment that tests exactly that step and nothing more:

- two Ray actors instead of one;
- one shared island instead of per-actor isolated runtimes;
- actor→actor dispatch instead of an external prober driving all actions;
- both directions, so neither actor is a passive endpoint.

It deliberately keeps the operation synthetic. Whether such a topology can carry a *useful*
distributed workload is the next gate (exp68); how the two orchestration paths compare under
matched measurement is the gate after that (exp69).

---

## 3. Architecture and mechanism

### Roles and processes

```text
controller (run_exp67.py)          — creates actors, launches root, evaluates gates
exp67_peer --role root             — hpx::init(console): work-free AGAS root, locality 0
Ray actor A worker  ── in-process HPX locality 1  (hpx::start(connect))
Ray actor B worker  ── in-process HPX locality 2  (hpx::start(connect))
```

- **Root** (`exp67_peer.cpp`, the only peer role — exp67 has no prober): a separately
  supervised, work-free island anchor. Its `hpx_main` only heartbeats `root.alive`, observes
  membership (expected to reach 3), and finalizes on the `root.done` sentinel after a bounded
  wait for every connector to leave — the exp63/64/65/66 user-space lifecycle protocol, so
  finalize never races a connector's `post(disconnect)+stop`. No application action ever
  targets locality 0 (gate `root_runs_no_application_action`).
- **Actors A and B** each import `exp67_actor_ext` and call
  `start_connect(hpx_threads, extra_args)` → `hpx::start(nullptr, argc, argv, params)` with
  `params.mode = hpx::runtime_mode::connect` and the GIL released. The HPX runtime lives on
  background threads of each actor worker; no child process is created (per-actor
  `child_report()` scan, gate `both_in_process_no_hpx_child`).

### The bidirectional action path

Each actor exposes `dispatch_to(x, target_loc, bound_s)`, which sends three registered
`HPX_PLAIN_ACTION`s **at** the peer's locality via `hpx::async<...>(target, ...)`:

- `exp67_pid_action` — returns the executing **process PID**;
- `exp67_probe_action` — returns the closed oracle
  `probe_value(x, loc) = (x ^ 0x67C0DE) + (loc << 1)` (exp67 uses a distinct XOR constant so
  its results can never be confused with exp66's), reproduced independently in Python;
- `exp67_host_action` — returns the executing process's **hostname**, reported over the HPX
  plane so the destination-host witness is genuinely remote, not controller-inferred.

The A→B call must return B's PID, B's locality-encoded oracle value, and B's hostname; B→A
symmetrically proves A. Self-probes are explicitly **not** accepted as remote-peer proof
(fence `self_probe_not_accepted_as_remote_peer_proof`).

### Why the operation path is HPX, not Ray

By construction: the controller holds Ray handles to A and B, but **A and B hold no Ray
handle to each other** — the aggregate records this as `operation_over_hpx_not_ray` with the
note "the only channel between their localities is the HPX parcelport." A Ray-mediated
shortcut for the actor-to-actor probe is therefore structurally impossible, without claiming
wire-level instrumentation of the parcelport itself.

---

## 4. Methodology and gates

Both phases run 3 repetitions; the verdict rule is **PASS iff every gating slice passes in
every rep** (the saturation diagnostic is non-gating).

- **Slice A — shared in-process hosting** (local 11 gates; cross-node 19): fixed HPX commit,
  both actors started HPX in their own worker (`actor_a/b_started_in_worker`), two distinct
  Ray processes, distinct locality IDs, no HPX child in either actor, shared island
  membership, no static locality count on any argv, root isolated on locality 0, evidence
  completeness — plus, cross-node: three-node Slurm allocation, root on the root node, hard
  placement of each actor (`actor_a/b_hard_placed`, `NodeAffinitySchedulingStrategy(soft=False)`),
  distinct actor nodes, three roles on three nodes, endpoints pinned to the selected subnet,
  and a remote orphan check.
- **Slice B — bidirectional actor-to-actor actions** (local 12 gates; cross-node 16): both
  dispatch directions ready; B's PID proven by A→B and A's by B→A; both oracles match; each
  direction executes on the intended destination locality; hostname witnesses; root runs no
  application action; `operation_over_hpx_not_ray` — plus, cross-node: both directions cross
  nodes and each destination hostname is the intended node's.
- **Slice C — progress while the destination's Python thread is idle** (3 gates): each
  direction progresses with the destination actor's Python thread idle, and no Python polling
  loop anywhere.
- **Slice D — lifecycle** (local 9 gates; cross-node 11): graceful leave for both actors
  (`post(disconnect)+stop`, rc 0 both), root finalized clean, heartbeat/completion protocol,
  both actors destroyed and **recreated** (cross-node: recreated on the intended nodes), no
  orphans, all waits bounded.
- **Saturation diagnostic (non-gating):** each direction was also exercised while the
  *destination* actor's Python thread ran a tight CPU loop; all reps in both phases
  classified `progressed_under_dest_saturation`. The aggregate's note is explicit that this
  is not a GIL verdict — the actions are pure C++ and a free-threaded comparison would be
  needed to attribute causality.

---

## 5. Accepted results

### Local phase (single macOS host, TCP loopback)

| item | value |
|---|---|
| repetitions | 3/3 pass, `overall = pass`, zero failed gates |
| topology | root (locality 0) + actor A (locality 1) + actor B (locality 2), one host |
| membership | reached 3 in every rep (`root_final.max_membership = 3`), back to 1 before finalize |
| PID identity | A→B returned B's exact worker PID and B→A returned A's, all reps (47192/47195, 47246/47250, 47291/47294) |
| oracle | `probe_result` 6799581 (locality 2) and 6799579 (locality 1) for `x = 7`, both directions, all reps |
| lifecycle | both actors: stop rc 0, destroyed, recreated with fresh PIDs; root `finalized_clean`; no orphans |

### Cross-node phase (three Rostam nodes, accepted job 170744)

| item | value |
|---|---|
| topology | root **medusa00** (10.42.5.30, locality 0) · actor A **medusa01** (10.42.5.31, locality 1) · actor B **medusa11** (10.42.5.41, locality 2) |
| repetitions | 3/3 pass, `overall = pass`, zero failed gates, `failure_class = pass` every rep |
| placement | hard `NodeAffinitySchedulingStrategy(soft=False)` per actor; three distinct Ray node IDs; driver on the root node |
| bidirectional proof | A→B returned B's exact PID + oracle + `medusa11` FQDN; B→A returned A's exact PID + oracle + `medusa01` FQDN — every rep, both directions crossing nodes |
| membership | 3 at peak in every rep; back to 1 before root finalize (`leave_observed = true`) |
| in-process proof | PID equality for **both** actors + empty `hpx_children` scans for both |
| lifecycle | graceful leave both actors, root `finalized_clean`, both actors recreated **on their intended nodes** with fresh PIDs |
| orphans | none — Ray and peer orphan checks clean on all three nodes (`remote_orphan_check_ran`) |

---

## 6. Interpretation

What the accepted evidence establishes:

- **The exp66 hosting mechanism composes.** Two independent Ray actor workers can each run
  `hpx::start(connect)` in-process and become members of the same HPX island; nothing about
  in-process hosting is single-instance-only in the tested configuration.
- **Ray placement and HPX identity line up end-to-end.** Hard-placed Ray actors on distinct
  nodes yield distinct HPX localities whose PID, hostname, and locality witnesses agree with
  Ray's own placement records, in every repetition.
- **The application path can be HPX while supervision stays Ray.** With no mutual Ray handle,
  the verified actor-to-actor probes could only have traveled the HPX parcelport. Ray still
  created, health-checked, destroyed, and recreated both actors.
- **The destination's Python thread is not in the action path.** Both directions progressed
  while the destination's Python thread was idle (gating) and while it was saturated
  (non-gating diagnostic), consistent with pure-C++ actions served by in-process HPX runtime
  threads.
- **The multi-connector lifecycle protocol holds.** The bounded root-leave ordering proven in
  exp63–66 extends to two simultaneous Ray-hosted connectors: membership 1 → 3 → 1 with a
  clean root finalize in every rep.

What remains ambiguous / untested here: any useful workload shape (exp68), any performance
property (exp69), more than two actor localities, concurrent membership churn, and every
ungraceful failure path.

---

## 7. Limitations and non-claims

Limitations of the tested configurations:

- exactly **two** Ray-actor localities plus one root; no larger topology;
- synthetic closed-`int64`/hostname probe actions — not a real workload, not LLM-shaped;
- clean-path lifecycle only: no failure injection, crash, eviction, or churn;
- CPU-only; GIL builds (CPython 3.11.15 local / 3.12.3 cross-node); free-threaded and
  Python-3.14 scope explicitly deferred (`deferred_not_started` in both aggregates);
- fixed software stack (Ray 2.55.1, HPX commit `20bc3d4bf3…`, Rostam medusa nodes).

Explicit non-claims (mirroring the aggregates' claim fences):

- no performance, speedup, ratio, or winner claim — recorded durations are observational only;
- no elasticity, churn, or recovery claim;
- no production-API claim — this is an experiment-only probe, not shipped `rayx.runtime` API;
- no wire/serialization instrumentation claim — "operation over HPX, not Ray" is proven by
  construction (no mutual handle), not by packet capture;
- self-probes are never accepted as remote-peer proof;
- no general claim that arbitrary Ray actor sets can share arbitrary HPX runtimes — the proof
  is scoped to this two-actor connect-mode topology.

---

## 8. Relationship to exp66 and exp68

- **exp66** (predecessor): one Ray actor hosts a networking HPX locality in-process. exp67
  reuses its hosting mechanism, PID-identity proof style, child-process scan, closed-oracle
  discipline, and root lifecycle protocol.
- **exp67** (this experiment): two Ray actors, one shared HPX runtime, bidirectional
  actor-to-actor HPX actions, three-node hard placement.
- **exp68** (successor): the same two-actor shared-runtime topology carries a useful,
  exactly-checkable distributed workload — deterministic vocabulary-sharded top-k — with
  bit-level verification against an independent oracle.
- **exp69** then characterizes Ray-mediated vs HPX-mediated orchestration of the exp68
  workload under matched resources at the same Python caller boundary.

exp67 is the bridge: without it, exp68's workload would have no proven substrate.

---

## 9. Evidence and reproducibility

**Accepted jobs and runs:**

| phase | job / run | reps | result |
|---|---|---|---|
| local (macOS, loopback) | run `_exp67_runs/20260714T170244Z/` | 3 | `overall = pass` |
| cross-node smoke | Slurm job **170743** (medusa[00-01,11]) | 1 | pass → `_exp67_runs/smoke_crossnode_agg.json` (gitignored) |
| cross-node accepted | Slurm job **170744** (medusa[00-01,11]) | 3 | `overall = pass` → tracked cross-node aggregate |

**Tracked curated evidence (live MD5):**

| artifact | phase | MD5 |
|---|---|---|
| `two_ray_actors_shared_hpx_aggregate.json` | local | `796481959eb58186f9999acc07c625a1` |
| `two_ray_actors_shared_hpx_crossnode_aggregate.json` | cross-node (170744) | `45e73515f81e572fc9c1e3b8209ee5b3` |

Also tracked: `run_exp67.py`, `actor_ext.cpp`, `exp67_peer.cpp`, `probe_action.hpp`,
`shared_probe.hpp`, `CMakeLists.txt`, `exp67_crossnode.sbatch`, `.gitignore`.

**Gitignored raw evidence** (`_exp67_runs/`): per-rep marker directories (local
`20260714T…Z/`, cross-node `crossnode_170743_*` and `crossnode_170744_*`), the Slurm outputs
`exp67xn_170743.out` / `exp67xn_170744.out`, and `smoke_crossnode_agg.json`. Build outputs
(`build*/`, `*.so`, `*.o`, `__pycache__/`) are ignored.

**Provenance:**

- **HPX** commit `20bc3d4bf3068383edcb63be13f22e9ff95842fa` (HPX V2.0.0, AGAS V3.0, Hwloc
  2.12.0), asserted by value before measurement (`fixed_hpx_commit`). Two per-platform
  release builds at that commit: local macOS (Apple Clang 17.0.0, Boost 1.90.0) and Rostam
  linux (GNU C++ 15.1.0, Boost 1.91.0). `actor_ext_abi_match: true` on both.
- **Ray** 2.55.1 (commit `237c2455ebb1ea15a32dd9e1fdeb2d617badc37f`).
- **Python** — local CPython **3.11.15** (macOS arm64, 10 cores); cross-node CPython
  **3.12.3** (Linux x86_64, 40 cores). Both GIL builds; `free_threaded_build: false`.
- **Ray node IDs (170744):** root `50628b5474…` (medusa00), actor A `9c00f23789…`
  (medusa01), actor B `1a77456a6f…` (medusa11); driver on the root node.
- **Cross-node actor PIDs (all HPX-proven by the peer direction):** rep 1 A `1362303` /
  B `3744772`; rep 2 A `1362551` / B `3745010`; rep 3 A `1362768` / B `3745226`.

**Provenance note (documentation-only):** like exp66, the cross-node aggregate's
`claim_fences` dict carries `not_cross_node: true`, an un-flipped default inherited from the
shared fence template. It contradicts the run's own topology (`cross_node: true`,
`single_node: false`, job 170744 on three nodes) and its `safe_claim`. The run **is**
cross-node; the flag is a metadata artifact only, recorded here for transparency.
