# exp63 — HPX-native composition: diagnosis and cross-node validation (experiment-only)

Durable overview of the exp63 arc. Three focused sub-reports carry the detailed evidence:

- [`connector_lifetime_hardening.md`](connector_lifetime_hardening.md) — the root-cause
  diagnosis and the minimal lifetime fix, with a same-allocation A/B;
- [`native_composition_retest.md`](native_composition_retest.md) — Slice 2a: both native
  composed waits validated cross-node under the hardened contract;
- [`tree_of_partials_retest.md`](tree_of_partials_retest.md) — Slice 2b: the depth-2
  root-of-partials fan-in topology validated on the same substrate.

exp63 is HPX-only mechanism/lifecycle/topology evidence. It contains no Ray comparison, no
payload axis, no collectives validation, no same-axis measurement, and no performance
claim.

---

## 1. Executive summary

**Problem.** exp62's cross-node HPX fan-in had to ship on an interim polled composition
(`root_flat_gather_poll`) because the HPX-native alternative
(`hpx::when_all(...).then(reduce)`) was mathematically correct yet stalled its composed
future to the dispatch timeout cross-node — an apparent native progress failure that, left
unexplained, would have undermined the whole HPX-native composition direction.

**Answer.** The failure was **not an intrinsic HPX progress defect**. It was a **connector
serve-window / lifecycle race**: a fixed wall-clock serve-timeout let a connector's HPX
thread pool stop while the root was still dispatching, so a late leaf parcel hit
`create_work` on a stopped pool (`invalid_status: thread pool is not running`), surfacing
at the root as a misleading `std::system_error` "Operation not permitted". A minimal
lifetime hardening — per-dispatch root heartbeats plus a root-completion sentinel, with
`serve-timeout` demoted to a deadman guard against root silence — removed the fault.

**Evidence.** A syscall trace with zero EPERM; a hold-open pass; a monotonic serve-timeout
sweep (fault index scales with the window, then plateaus to a clean pass); a
same-allocation hardened-vs-control A/B in which the control reproduced the exact original
fault and the hardened run passed 20/20. Under the hardened contract, both native composed
waits (`when_all_then_reduce`, `dataflow_reduce`) then validated cross-node 20/20 with
correct oracles (Slice 2a), and a depth-2 star / root-of-partials fan-in validated on the
same substrate (Slice 2b).

**Limitations.** Closed-`int64` mechanism scope at N=8 across two remote localities;
validation holds *under the hardened lifetime contract* and does not claim robustness under
all timings, transports, or scales; `hpx::collectives::reduce` remains unvalidated later
work with known membership/timeout/poison hazards.

---

## 2. Motivation

exp62 left two caveats: an interim non-native fan-in composition, and closed-int64-only
payloads. exp63 targets the first. An HPX-expert review redirected the plan away from
starting with `hpx::collectives::reduce`: collectives ride the same future/TCP-parcelport
substrate as the stalled spike (so they could reproduce, not fix, a passive-wait stall) and
add fragility (fixed membership, communicator generations, no built-in timeout, island
poisoning on a lost participant — echoing exp50/exp51). The first variable to isolate was
runtime progress and lifecycle, not the composition primitive.

---

## 3. Methodology (slice ladder)

- **Slice 0** — pure-Python scaffold: closed-int64 oracles (placement-independent by
  construction), composition/progress/payload provenance builders with fail-closed
  consistency gates, distribution witnesses, hard fences locked false, hermetic selftests,
  clean off-cluster skips.
- **Slice 1** — experiment-only C++ (`collective_ext` embedded AGAS root + leaf-action
  fanout with selectable composition modes; `collective_connector` connect-mode remote
  locality), copied from the proven exp62 C++ with a distinct action namespace.
- **A1 diagnosis + hardening** — reproduce the native-mode fault under instrumentation,
  diagnose, fix, and prove with an A/B (first sub-report).
- **Slice 2a** — retest the two native composed waits end-to-end under the hardened
  contract, with the poll as a mechanics control that can never claim native validation
  (`polled_in_success_path=true` by construction) (second sub-report).
- **Slice 2b** — change the fan-in **topology**, not the wait: each remote locality folds
  its contiguous leaf block locally and returns one partial, so the root composes r=2
  partial futures instead of N=8 leaf futures. Hand-rolled per-locality action futures are
  used deliberately instead of collectives (no fixed communicator; a failed locality
  surfaces as an exception through its future rather than hanging a communicator) (third
  sub-report).

---

## 4. Accepted results (summary)

| stage | job | topology | result |
|---|---|---|---|
| hold-open confirmation | 159058 | 3-node medusa | 20/20 pass with serve window held open |
| serve-timeout sweep | 159059 | 3-node medusa | fault index 7 @ 90 s → 14 @ 150 s → pass @ 300/600 s (race signature) |
| hardened-vs-control A/B | 159061 | same allocation | hardened 20/20 @ 90 s; control reproduces call-7 fault |
| Slice 2a native retest | 159167 | medusa06 + medusa07/08 | `when_all_then_reduce` and `dataflow_reduce` both 20/20, `cross_node_composition_validated=true` |
| Slice 2b root-of-partials | 159200 | medusa11 + medusa12/13 | both collect waits 20/20; blocks [4,4] tile [0,8) exactly once; validated |

(An earlier strace run, job 159057, established the zero-EPERM evidence.) In every accepted
run: correct closed-int64 oracle, all leaves remote across two localities, both connectors
leaving via `root_completion_signal`, no late parcels, fences locked false,
`same_axis_comparison=false`.

---

## 5. Interpretation

- The exp62 native-composition negative is **resolved**: the stall was dominated by
  connector lifetime, and with connectors guaranteed alive for the dispatch window, passive
  composed waits progress cross-node in this setup.
- Lifecycle contracts are load-bearing in connect-mode islands: "who may stop, and when"
  must be an explicit protocol (heartbeat + completion sentinel), not a wall-clock guess.
  This contract became the root/connector lifecycle protocol reused by exp64–68.
- The root-of-partials shape puts a structurally different fan-in on record (root cost
  bounded by locality count, not leaf count; edge-side combine) for later larger-N design —
  a structural observation, not a measured win.
- `root_flat_gather_poll` remains a known-good mechanics control and the composition that
  exp62's accepted band actually measured.

---

## 6. Limitations and non-claims

- HPX-only; no Ray arm anywhere in exp63; no same-axis evidence; no performance, latency,
  ratio, or winner statement of any kind.
- Closed-`int64` values only; no payload-size or serialization evidence (exp64's axis).
- Native validation is scoped to the hardened lifetime contract at N=8, r=2 on this TCP
  setup; no robustness claim beyond it.
- Collectives remain unvalidated; their membership/generation/timeout/poison design is
  future work.
- Experiment-only code; not shipped `rayx.runtime` API, no object store, no arbitrary
  remote Python, not Ray Serve, not real inference.

---

## 7. Relationship to neighboring experiments

- **exp62** (predecessor): produced the informative negative this arc resolves; its
  accepted band remains on the polled composition.
- **exp64** (successor): reuses the hardened connector-lifetime contract and adds the
  payload axis; its timed-wait discriminator later isolated a separate upstream HPX
  readiness bug (distinct from this lifetime race).
- **exp65–68**: the heartbeat/completion lifecycle protocol proven here is the basis of the
  root/connector lifecycle used throughout the Ray-hosted island arc.

---

## 8. Evidence and reproducibility

**Jobs:** 159057 (strace), 159058 (hold-open), 159059 (sweep), 159061 (A/B), 159167
(Slice 2a), 159200 (Slice 2b) — all on Rostam medusa nodes, subnet `10.42.5.`,
`--exclusive` 3-node allocations for the retests.

**Tracked:** this overview, the three sub-reports, `run_exp63_collective.py`,
`selftest_slice0.py`, the C++ sources, and `CMakeLists.txt`.

**Gitignored raw evidence** under `_exp63_runs/`:
`a1_strace_copyback_159057/`, `a1_holdopen_clean_copyback_159058/`,
`a1_serve_timeout_sweep_copyback_159059/`, `a1_lifetime_fix_copyback_159061/`,
`slice2a_native_retest_copyback_159167/`, `slice2b_tree_copyback_159200/` (per-mode
artifacts with job/mode/timeout/port encoded in filenames, plus tee logs). Build outputs
are ignored.

Reproduction command lines, per-mode artifact filenames, and the added connector-lifetime
artifact fields are recorded in the three sub-reports.
