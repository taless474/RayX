# exp66 — A networking HPX locality hosted in-process inside one Ray actor worker

Durable experiment-local account of exp66. This is the standing record of the process
model, mechanism, correctness/lifecycle gates, cross-node evidence, and claim boundaries.

exp66 is an **architectural and lifecycle gate**, not a performance comparison. All
numbers, job IDs, hashes, node IDs, PIDs, and source names below were verified against the
two curated aggregates and the copied-back evidence in this directory.

Directory: `experiments/66_hpx_runtime_inside_ray_actor/`.

---

## 1. Executive summary

The question exp66 answers:

> Can one Ray actor worker host a networking-capable HPX locality **in-process** — join an
> HPX runtime, execute HPX work, and shut down cleanly — **without spawning or delegating
> the locality to a child process**?

**Verdict: yes**, in the tested local and cross-node configurations (3/3 passing reps each).
A Ray actor imports a native extension and calls `hpx::start` in `runtime_mode::connect`,
which brings a networking (TCP parcelport) HPX runtime up **on background threads of the
actor worker process itself**. The locality:

- runs **inside the Ray actor worker process** (proven by exact PID equality);
- has **no child HPX process** (proven by a child-process scan);
- lets **Ray own** actor placement, process lifecycle, and supervision;
- lets **HPX own** its runtime, TCP networking, locality identity, and action execution.

A separately supervised, work-free HPX root (locality 0) anchors the island; an external
standalone prober dispatches closed-oracle HPX actions **at** the actor locality and checks
them by value. Every accepted rep proved same-process hosting, correct remote action
execution, HPX progress while the actor's Python thread was idle, graceful teardown, actor
recreation, and no orphans.

---

## 2. Why exp66 was needed

Earlier RayX work established HPX and Ray mechanisms **separately or through external
coordination**: the distributed-island arc (exp49–52, exp57–65) ran HPX localities as
standalone processes that Ray (or Slurm) launched and supervised, with the HPX action/data
path carried by those separate HPX processes. Ray hosted or bootstrapped HPX; it never
*became* an HPX locality.

exp66 tests whether the integration boundary can be moved **inside the Ray actor process
itself** — i.e., whether a Ray actor worker can *be* a networking HPX locality rather than
merely launch or talk to one. This is the prerequisite for the shared-runtime design: if a
single actor cannot host HPX in-process cleanly, two actors cannot share one HPX runtime
(exp67), nor run a distributed workload over it (exp68), nor be compared on matched
orchestration paths (exp69). exp66 is the first, narrowest gate in that chain.

---

## 3. Exact process model

Processes involved (cross-node phase; the local phase collapses them onto one host):

- **Controller / driver** — the Python process running `run_exp66.py`. It creates the Ray
  actor, launches the standalone root and prober, writes the rendezvous/trigger files, and
  evaluates gates. Cross-node: on the head node (medusa00).
- **Ray head + worker** — a two-node Ray cluster (`ray start`). The actor worker process is
  where HPX is hosted.
- **Actor worker process** — hosts the HPX locality in-process.
- **Standalone `exp66_peer` binary**, two roles (one binary, exp65 pattern):
  - `--role root` — `hpx::init` with `runtime_mode::console`: the **work-free AGAS root /
    locality 0**. Its `hpx_main` only heartbeats `root.alive`, witnesses membership, and
    finalizes on the `root.done` sentinel after a bounded wait for connectors to leave. No
    application action ever targets locality 0.
  - `--role prober` — `hpx::start` with `runtime_mode::connect`: a standalone connect-mode
    locality (its main thread stays non-HPX). On the controller's per-phase trigger files it
    dispatches the fixed exp66 actions **at the Ray-actor locality** and writes one result
    JSON per phase.

**HPX-in-actor API:** the actor imports `exp66_actor_ext` and calls
`start_connect(hpx_threads, extra_args)` → `hpx::start(nullptr, argc, argv, params)` with
`params.mode = hpx::runtime_mode::connect` and the GIL released. This is `hpx::start`
(background-threaded), **not** `hpx::init`. No child process is created — the runtime lives
on background threads of the actor worker.

**Same-process PID proof (two independent witnesses):**
1. `exp66_pid_action` (an `HPX_PLAIN_ACTION` over `getpid()`) is dispatched by the prober
   **at the actor locality**; the returned pid is compared to the Ray actor's own
   `os.getpid()` (gate `actor_pid_equals_locality_pid`). Equality means the HPX action ran
   in the actor's own process.
2. The actor's `child_report()` enumerates its **direct child processes** and flags any that
   look like an HPX binary; the gate `no_hpx_child_process` requires `hpx_children == []`.

The strong proof is the PID equality; the child scan is the corroborating negative. The
actor PID is also confirmed stable within a rep via `actor_identity.pid` == the health-check
PID (`ray_worker_healthy`).

---

## 4. Local topology

- **Host:** single macOS host (`Bitas-MacBook-Air.local`, arm64, 10 cores); `single_node =
  true`, transport `tcp_loopback`.
- **Arrangement:** the work-free root (locality 0), the standalone prober (a separate
  connect-mode locality), and the one Ray actor (hosting HPX in-process) are all co-resident
  on the host over the loopback TCP parcelport.
- **Locality IDs:** actor locality **= 1** (nonzero, connect mode); root **= 0**; the prober
  is a separate nonzero connect-mode locality. The actor's start witness recorded
  `membership ≥ 2`.
- **Placement:** local Ray actor (no cross-node placement).
- **Repetitions:** 3.
- **Workload/action:** the closed-oracle `exp66_probe_action` + `exp66_pid_action`,
  dispatched by the prober at the actor locality in an **idle** phase (gating) and a **busy**
  phase (diagnostic).
- **Gates:** the full Slice A (19) + Slice B (3) gate set (see §9).

**Proves:** the in-process hosting mechanism, PID identity, no-child, remote action + oracle
on the actor locality, idle progress, and clean lifecycle work on one host.
**Does not prove:** cross-node placement, real inter-node network transport, or distinct
physical hosts (all localities share one host over loopback).

---

## 5. Cross-node topology (accepted job 170524)

| item | value |
|---|---|
| Slurm job | **170524** (nodelist `medusa[00-01]`) |
| nodeA — head / driver / root+prober | **medusa00** (`10.42.5.30`) |
| nodeB — Ray worker / **actor host** | **medusa01** (`10.42.5.31`) |
| Ray node IDs | nodeA `da6cfc33af…`, nodeB `20a143904a…` (distinct) |
| driver | on nodeA (`driver_on_nodeA: true`, `medusa00.rostam.cct.lsu.edu`) |
| actor hostname (witness) | `medusa01.rostam.cct.lsu.edu` |
| actor PID (rep 1 witness) | 1342471 (== HPX-executed pid across nodes) |
| actor HPX locality ID | 1 |
| HPX endpoint / transport | TCP parcelport over `10.42.5.x` (`tcp_cross_node`) |
| subnet | `10.42.5.` |
| placement | hard `NodeAffinitySchedulingStrategy(soft=False)`, resolved Ray node-id + FQDN-normalized hostname |

The **root is on a separate node** (medusa00) from the actor (medusa01) and is **work-free**
(locality 0; no probe action ever executed there — gate `root_work_free` +
`root_and_actor_different_nodes` + `root_on_root_node`). This is a **two-node** topology
(the head/root/prober share medusa00; the actor is alone on medusa01).

---

## 6. HPX startup inside the actor

Per rep, the exact sequence:

1. **Ray creates and places** the actor (cross-node: hard-placed on medusa01).
2. The actor **imports the native extension** `exp66_actor_ext` and reads its HPX build
   identity via `hpx_version_info()` — callable **before** start — so the controller can
   assert the verified fixed HPX build before any measurement.
3. The actor **starts HPX in-process**: `start_connect(hpx_threads=2, …)` →
   `hpx::start(runtime_mode::connect)` with the GIL released; the runtime comes up on
   background threads. Endpoints are pinned to the selected `10.42.5.x` interface
   (`endpoints_pinned_subnet`).
4. The locality **joins the island** and reports `locality_id() > 0` (= 1) and
   `membership ≥ 2` (`connect_mode_admission`).
5. **Readiness** is proved by the actor's own `locality_id`/`membership` witnesses plus the
   root/prober rendezvous markers (`root.ready`, `prober.joined`).
6. **HPX work is invoked**: the prober dispatches `exp66_pid_action` and
   `exp66_probe_action` **at** the actor locality (bounded `wait_for`, `.get()` only when
   ready).
7. **Results and witnesses** return (pid, oracle value, `executed_on` locality).
8. The connector **leaves**: `post(hpx::disconnect)` + `hpx::stop` (exp49-proven idiom).
9. **Actor teardown** (destruction + recreation) and **orphan scans** run.

Special handling recorded in source/markers: explicit `--hpx:threads` on the connect-mode
command line; endpoint/subnet pinning to `10.42.5.x`; root discovery via the shared
bootstrap/rendezvous directory (NFS-visible markers `root.ready` / `root.alive` /
`root.done` / `actor.loc`); a bounded root-leave ordering so finalize never races a
connector's `post(disconnect)+stop`; and HPX networking on background runtime threads (the
actor's Python thread does not drive HPX progress).

---

## 7. Operation executed

The functional proof is a **closed-oracle HPX action**, not a benchmark.

- **Registered action:** `exp66_probe_action` ← `exp66_probe(x)` returns
  `probe_value(x, hpx::get_locality_id())`. Also `exp66_pid_action` ← `exp66_pid()` returns
  `getpid()`. Both are `HPX_PLAIN_ACTION`s registered in exactly one TU per binary.
- **Argument:** a closed `int64` input `x` (default 7).
- **Expected result / oracle:** the deterministic closed value
  `probe_value(x, loc) = (x ^ 0x66C0DE) + (loc << 1)`, reproduced independently in Python
  (`run_exp66.py::probe_value`). `executed_locality_from(result, x)` recovers the executing
  locality from the returned value.
- **Witnesses:** `pid_result` (executing process pid), `executed_on` (recovered locality),
  `oracle_match` (value equals the Python oracle), plus the actor's own pid/hostname/locality.
- **Execution is through an HPX action** (`hpx::async<exp66_probe_action>(target, x)`), where
  `target` is the actor locality resolved from `find_all_localities()`.
- **Fake / local execution is ruled out** by three coupled checks: `executed_on` must equal
  the actor locality (which is nonzero, so it is not the root and not a same-caller shortcut);
  `oracle_match` must hold for `probe_value(x, actor_loc)` (the value encodes the executing
  locality, so a wrong locality yields a wrong value); and `pid_result` must equal the Ray
  actor's `os.getpid()`. The prober is a *different* process/locality from the actor, so a
  successful dispatch is a genuine cross-locality (cross-node, in the accepted phase) action.

This is not accepted as performance evidence; recorded wall-ms fields are observational only.

---

## 8. Same-process proof

The central exp66 claim rests on these witnesses, all required true in every rep:

- **Ray actor worker PID** — `actor_identity.pid` (the actor's `os.getpid()`).
- **HPX code-execution PID** — `idle_probe.pid_result`, the value returned by
  `exp66_pid_action` executed **on the actor locality** and dispatched by the external prober.
- **Equality** — gate `actor_pid_equals_locality_pid`: `pid_result == actor_pid` with
  `both_ready` true. Verified values:

  | phase | rep | actor PID | HPX-executed PID | equal? | locality | oracle |
  |---|---|---|---|---|---|---|
  | local | 1 | 39393 | 39393 | ✓ | 1 | ✓ |
  | local | 2 | 39391 | 39391 | ✓ | 1 | ✓ |
  | local | 3 | 39452 | 39452 | ✓ | 1 | ✓ |
  | cross-node | 1 | 1342471 | 1342471 | ✓ | 1 | ✓ |
  | cross-node | 2 | 1342719 | 1342719 | ✓ | 1 | ✓ |
  | cross-node | 3 | 1342936 | 1342936 | ✓ | 1 | ✓ |

- **Actor hostname** — `actor_identity.hostname` (`Bitas-MacBook-Air.local` local;
  `medusa01.rostam.cct.lsu.edu` cross-node); cross-node the actor-node hostname is a
  dedicated gate (`hostname_witness_actor_node`).
- **HPX locality hostname** — the executing locality's hostname agrees (the action ran on
  the actor's host, not the root's).
- **Locality ID** — actor locality `= 1` (nonzero → connect mode, not the root's locality 0).
- **Absence of a child locality process** — `no_hpx_child_process` (the actor's direct-child
  scan found zero HPX children).
- **Stable PID throughout the rep** — the actor identity PID equals the health-check PID
  (`ray_worker_healthy`) and equals the HPX-executed PID.

Accepted conclusion, stated narrowly:

> The HPX locality executed inside the Ray actor worker process.

This is scoped to the tested single-actor topology and configuration; it is **not**
generalized to all Ray actors or all HPX configurations.

---

## 9. Correctness and acceptance gates

Verdict rule: **PASS iff all Slice A and all Slice B gates pass in every rep** (Slice C is
diagnostic-only; Slice D is deferred).

**Slice A — in-process hosting + full lifecycle (all gating).** Local: **19** gates —
`fixed_hpx_commit`, `hpx_started_in_worker`, `actor_pid_equals_locality_pid`,
`hostname_identity`, `no_hpx_child_process`, `connect_mode_admission`,
`no_static_locality_count`, `root_work_free`, `remote_action_on_actor`,
`exact_oracle_and_witness`, `ray_worker_healthy`, `heartbeat_completion_lifecycle`,
`graceful_leave`, `clean_hpx_stop_in_process`, `root_finalized_clean`, `actor_destruction`,
`actor_recreation`, `no_orphans`, `evidence_complete`.

**Cross-node adds 8 placement/identity gates** (26 total): `two_node_slurm_allocation`,
`root_on_root_node`, `root_and_actor_different_nodes`, `actor_hard_placed_on_actor_node`,
`hostname_witness_actor_node`, `endpoints_pinned_subnet`, `remote_orphan_check_ran`,
`evidence_fields_complete_crossnode`.

**Slice B — HPX progress while the actor's Python thread is idle (all gating, 3 gates):**
`idle_progress_oracle`, `actor_python_idle_during_dispatch`, `no_python_polling_loop`.

**Slice C — actor-thread CPU/GIL saturation (diagnostic, non-gating, `affects_verdict:
false`):** classifies whether the HPX action progressed while the actor's Python thread ran a
tight CPU loop. It **never** attributes a stall to the GIL without a free-threaded
comparison to separate GIL monopolization from thread-budget/oversubscription effects.

**Slice D — free-threaded (t-ABI) comparison:** `deferred_skipped` (no free-threaded
interpreter / Ray environment available).

**Accepted totals:** local **3/3** reps pass, `overall = pass`, zero failed Slice A/B gates;
cross-node **3/3** reps pass, `overall = pass`, zero failed Slice A/B gates.

---

## 10. Local accepted evidence

| metric | value |
|---|---|
| repetitions | 3 |
| passes | 3 |
| failures | 0 |
| value/oracle mismatches | 0 |
| PID-equality mismatches | 0 (39393, 39391, 39452 all self-equal) |
| locality mismatches | 0 (actor locality = 1, executed_on = 1 every rep) |
| timeouts / not-ready | 0 (`both_ready` true every rep) |
| lifecycle failures | 0 (graceful leave, root finalize, recreation, no orphans) |
| Slice C | `progressed_under_actor_saturation` ×3 (diagnostic) |

Host macOS `Bitas-MacBook-Air.local`, CPython 3.11.15, transport `tcp_loopback`.

---

## 11. Cross-node accepted evidence

| item | value |
|---|---|
| job ID | **170524** |
| nodes | medusa00 (root/prober/head), medusa01 (actor/worker) |
| repetitions | 3 |
| passes | 3 (`overall = pass`) |
| correctness totals | oracle-match 3/3; PID equality 3/3 (1342471, 1342719, 1342936); executed_on = actor locality 1, 3/3; 0 not-ready |
| process-identity verdict | PASS (`actor_pid_equals_locality_pid`, `no_hpx_child_process`) |
| placement verdict | PASS (hard-placed on medusa01; `actor_hard_placed_on_actor_node`, `root_and_actor_different_nodes`, `root_on_root_node`, `endpoints_pinned_subnet`, `two_node_slurm_allocation`) |
| root/membership verdict | PASS (root work-free on locality 0; `membership ≥ 2`; `root_work_free`) |
| lifecycle verdict | PASS (graceful leave, `root_finalized_clean`, actor recreation with fresh PID) |
| orphan verdict | PASS (`no_ray_orphans` and `no_peer_orphans` on nodeA and nodeB; `remote_orphan_check_ran`) |

CPython 3.12.3, Ray 2.55.1, HPX `20bc3d4bf3…`, TCP parcelport over `10.42.5.x`.

---

## 12. Lifecycle and cleanup

Per rep, exp66 exercises and gates the **clean-path** lifecycle:

- **Graceful locality leave** — the actor's HPX connector leaves via `post(hpx::disconnect)`
  + `hpx::stop` (`stop_rc == 0`, `clean_hpx_stop_in_process`); the prober leaves the same way
  with `shutdown_reason = root_completion_signal`.
- **Root membership transition** — the work-free root observes membership rise to ≥ 2, then
  waits (bounded) for connectors to leave and observes membership return to 1
  (`leave_observed: true`, `final_membership == 1`) before `hpx::finalize`.
- **Actor destruction** — the actor is destroyed (`actor_destruction`).
- **Actor recreation** — a fresh actor is created on the intended node with a **new PID**
  (`actor_recreation`; recreate PIDs differ from the original every rep), showing the Ray
  worker stayed usable across host/HPX teardown.
- **Ray worker cleanup** — Ray head/worker cleanup recorded on both nodes.
- **Root finalization** — `root_finalized_clean` (`root_exit_path == finalized_clean`).
- **Orphan scanning** — local orphan scan and, cross-node, a **remote** orphan check on both
  nodes; all clean.

exp66 tests **clean-path lifecycle only.** It does **not** inject ungraceful failure, crash,
eviction, or membership churn, and it does not claim restart/failure containment.

---

## 13. Engineering findings

Recovered from source, aggregates, and copied-back Slurm outputs:

- **HPX networking runs inside a Ray worker.** `hpx::start(connect)` brings a TCP-parcelport
  HPX runtime up on background threads of the actor worker process; no child process is
  needed for this integration model.
- **Actor process identity is stable and load-bearing.** The actor's `os.getpid()` equals
  the PID returned by an HPX action executed on the actor locality, in every rep, across
  nodes — the same-process property is provable by value.
- **The root must stay isolated and work-free.** Locality 0 only heartbeats and witnesses
  membership; no probe ever targets it, and `root_work_free` enforces that.
- **Selected network interfaces must be explicit.** Endpoints are pinned to the `10.42.5.x`
  subnet (`endpoints_pinned_subnet`); the connect-mode command line carries explicit HPX
  threads.
- **Lifecycle ownership must be coordinated.** HPX shutdown (`post(disconnect)+stop`) occurs
  **before** actor teardown, and the root's bounded leave-wait prevents finalize from racing
  a connector's disconnect.
- **Actions never touch the GIL.** The C++ actions are pure; Slice C observed
  `progressed_under_actor_saturation` (the HPX action executed and oracle-matched while the
  actor's Python thread ran a tight CPU loop) — recorded as a **diagnostic**, explicitly not
  a GIL verdict (a free-threaded Slice D is needed to attribute causality).
- **Orphan-check self-match provenance.** The first cross-node submission, **job 170520**,
  had all three reps pass individually but was scored `overall = fail`: the orphan check's own
  remote `pgrep -f <pattern>` matched its **own launcher argv** on the controller node. The
  checker was fixed to filter its own machinery, and the full 3-rep set was rerun as
  **170524** (`overall = pass`), the accepted run. (Both `_exp66_runs/crossnode_170520_*` and
  `…_170524_*` are present locally, gitignored.)

---

## 14. Evidence and reproducibility

**Tracked (committed) — curated evidence:**

| artifact | phase | live MD5 |
|---|---|---|
| `hpx_inside_ray_actor_aggregate.json` | local control | `53f835db8139be2fd026c924b9dcd038` |
| `hpx_inside_ray_actor_crossnode_aggregate.json` | cross-node accepted (170524) | `5acf8701954f24ac05c69ea0e36748af` |

Also tracked: `run_exp66.py`, `actor_ext.cpp`, `exp66_peer.cpp`, `probe_action.hpp`,
`shared_probe.hpp`, `CMakeLists.txt`, `exp66_crossnode.sbatch`, `.gitignore`.

**Gitignored (raw evidence under `_exp66_runs/`):** per-rep bootstrap/marker directories
(`root.ready`/`root.alive`/`root.final`, `prober.joined`/`prober.disconnected`,
`probe_idle.result`/`probe_busy.result`, `actor.loc`), the local raw runs
(`2026071*T*Z/`), the cross-node run dirs (`crossnode_170520_*` invalidated,
`crossnode_170524_*` accepted), and the Slurm outputs (`exp66xn_170520.out`,
`exp66xn_170524.out`). Build outputs (`build/`, `*.so`, `*.o`) are ignored.

**Provenance:**
- **HPX** commit `20bc3d4bf3068383edcb63be13f22e9ff95842fa` (HPX V2.0.0, AGAS V3.0, Hwloc
  2.12.0; release builds dated Jul 13 2026) — the verified waiter-fix build, asserted by
  value before measurement (`fixed_hpx_commit`). Two per-platform builds at that commit:
  local macOS (Apple Clang 17.0.0, Boost 1.90.0, built 17:07:05) and cross-node Rostam linux
  (GNU C++ 15.1.0, Boost 1.91.0, built 19:07:30). `actor_ext_abi_match: true` on both.
- **Ray** 2.55.1 (commit `237c2455ebb1ea15a32dd9e1fdeb2d617badc37f`).
- **Python** — local **3.11.15** (macOS arm64, 10 cores); cross-node **3.12.3** (Linux
  x86_64, 40 cores). Both GIL builds; `free_threaded_build: false`;
  `actor_gil_declaration: gil_used_default`.

**Provenance note (documentation-only, no aggregate change):** the cross-node aggregate's
`claim_fences` dict carries `not_cross_node: true`, an un-flipped default inherited from the
shared fence template. It contradicts the run's own topology (`cross_node: true`,
`single_node: false`, preflight job 170524 on medusa[00-01]) and its `safe_claim` ("across
two Rostam nodes … medusa00->medusa01"). The run **is** cross-node; the flag is a metadata
artifact only and is recorded here for transparency.

---

## 15. Safe conclusions

> Exp66 demonstrates that, in the tested local and cross-node configurations, a Ray actor
> worker can host a networking-capable HPX locality in-process, execute HPX work under that
> same process identity, and complete a clean lifecycle.

> The accepted cross-node evidence (job 170524) proves actor placement, HPX locality
> identity, same-process execution, and clean teardown for the tested single-actor topology.

Scope: local macOS/CPython 3.11.15 and two-node Rostam (medusa00 root/prober, medusa01
actor) CPython 3.12.3; HPX `20bc3d4bf3…`; Ray 2.55.1; a synthetic closed-`int64` probe.

---

## 16. Explicit non-claims

- No Ray-versus-HPX **performance comparison**; recorded durations are observational only.
- No speedup, ratio, or **winner** claim.
- No two-actor shared-runtime proof — that is **exp67**.
- No useful distributed application workload — that is **exp68**.
- No same-axis performance evidence — that is **exp69**.
- No ungraceful-failure recovery (clean-path lifecycle only).
- No dynamic elasticity or membership-churn conclusion.
- No root-loss tolerance (the root is separately supervised and work-free; its loss is not
  tested).
- No production-scale claim; not `rayx.runtime` API; not real inference (no model, tokenizer,
  or GPU).
- No general claim that **every** HPX configuration can run inside **every** Ray actor — the
  proof is scoped to this single-actor connect-mode topology.

*(The run is cross-node; see the §14 provenance note about the aggregate's `not_cross_node`
metadata flag.)*

---

## 17. Relationship to exp67–69

- **exp66** (this experiment): one Ray actor hosts a networking HPX locality in-process.
- **exp67**: two Ray actors host HPX localities that participate in **one shared HPX
  runtime**, with bidirectional actor-to-actor HPX actions.
- **exp68**: a useful, exactly-checkable distributed operation — a deterministic
  vocabulary-sharded top-k — runs across those two Ray-hosted localities.
- **exp69**: a matched-boundary **performance characterization** of the Ray-mediated vs
  HPX-mediated orchestration paths for the exp68 workload.

exp66 is the foundational gate; each later experiment depends on it without narrowing it.

---

## 18. Limitations

- One Ray actor locality (single-actor hosting only).
- Small topology (local single host; cross-node two nodes).
- Synthetic closed-`int64` probe operation (not a real workload).
- CPU-only; no GPU, no model, no inference.
- Clean lifecycle only; no failure injection, no crash/restart, no eviction.
- Fixed software stack (Rostam medusa nodes; HPX `20bc3d4bf3…`; Ray 2.55.1; CPython 3.11.15
  local / 3.12.3 cross-node; GIL builds).
- No dynamic membership churn and no elasticity.
- No performance comparison (Slice C is a non-gating diagnostic; Slice D free-threaded
  comparison is deferred).
