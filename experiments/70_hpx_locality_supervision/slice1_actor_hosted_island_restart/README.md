# exp70 Slice 1 — Ray-supervised whole-island restart of an actor-hosted HPX island

**Status:** complete. Mechanism/structural evidence only. Not a performance experiment, not a
Ray comparison, not production code, not a shipped RayX API. No ratio / speedup / winner
language.

## 1. Question and architectural boundary

Can the Ray supervision plane recover a useful island after the **unexpected loss of one
actor-hosted HPX locality** — without assuming any HPX-native failure detection, loss
notification, or recovery?

The architectural boundary under test is the exp66–68 hybrid: Ray owns placement, actor
lifecycle, supervision, and whole-island restart; HPX actions and futures own the distributed
operation/composition path inside the island. Slice 1 deliberately answers the question with
**whole-island replacement**, not recovery: after classification the old island (root and
surviving connector included) is discarded by policy, and a completely fresh island is
constructed and re-verified.

## 2. Topology (local and Rostam)

Per island epoch (exp66/67/68 mechanism, reused in place and unmodified): one separately
supervised, **work-free** `exp68_peer` root (console mode, AGAS root, locality 0) plus two Ray
actors each hosting an HPX connect-mode locality **in-process** via the `exp68_actor_ext`
pybind11 extension.

* Local phase: everything on one host over loopback, one local Ray instance.
* Rostam cross-node phase (`exp70_slice1_crossnode.sbatch`, batch step on node A):
  * node A (medusa00, `10.42.5.30`): Slice 1 controller/driver, Ray head, work-free root,
    actor A (locality 1);
  * node B (medusa01, `10.42.5.31`): actor B (locality 2) — **the failure-injection victim,
    remote by construction**;
  * subnet-bound TCP parcelport endpoints on `10.42.5.x`.

## 3. Two-epoch state machine

One controller run drives two island epochs under one persistent Ray supervision plane:

* **epoch 0:** clean start → membership 3 → deterministic workload verified → one connector
  actor terminated unexpectedly (controller-initiated SIGKILL of the cross-checked worker PID;
  exp53 lineage) → island classified failed **from observation only** → bounded post-loss
  observations recorded verbatim → whole-island teardown with **no graceful application
  completion on the poisoned island** (exp51/53 policy: no `root.done`, no healthy-island
  disconnect request).
* **epoch 1:** fresh root + fresh actors + fresh ports/boot directory → same workload verified
  → graceful exp68-style shutdown (`stop_disconnect` both actors, `root.done`, `root.final`,
  finalize) → no orphans.

Epoch isolation is gated: disjoint ports, fresh process/actor identities, per-epoch boot
directories, and no epoch-0 marker visible to epoch 1.

## 4. Failure injection and observation-only classification

The injection is recorded separately from all classification evidence: victim actor ID, Ray
worker PID, extension PID, HPX locality, Ray node ID, hostname, signal (SIGKILL), timestamps,
and every precondition (PID cross-check: extension PID == worker `os.getpid()` == Ray's worker
PID). The classifier is **injection-blind** (exp54 discipline): it never reads the injection
record, and classifies the island failed only from observations — victim PID disappearance,
Ray `ActorDiedError` on contact, and the absence of any clean-disconnect record. A graceful
leave must **not** classify as island failure (selftested).

## 5. Post-loss membership is an observational category

After the loss, the surviving locality and the root are probed with **bounded** operations
only, and the result is recorded verbatim under exactly one category: `membership_shrank`,
`membership_stale`, `membership_query_error`, `membership_query_timeout`, or
`membership_other`. **No category is required to pass.** All five accepted runs observed
`membership_stale` (survivor still reported membership 3), consistent with the absence of
HPX-native loss surfacing on this path; a `membership_shrank` observation elsewhere would be a
finding, not a failure.

Witness distinction (do not conflate):

* Slice 0 uses a **dispatch-driven** `root.alive` activity witness (bumped before each
  dispatch, exp63 semantics).
* Slice 1 uses an **externally and periodically refreshed root-liveness witness**: the
  work-free root refreshes `root.alive` from its root loop (~0.2 s period).
* Neither is an HPX-native heartbeat, HPX failure detection, or dispatch evidence of the other
  kind.

## 6. Whole-island teardown policy

After classification: no further application work is dispatched (gated), `root.done` is never
written for the poisoned epoch, no graceful completion is requested, the surviving actor and
old root are terminated, and all recorded epoch-0 PIDs must be gone. `root.final` absent is
**expected** for the poisoned epoch. Epoch-0 artifacts are retained.

## 7. Fresh replacement proof

Epoch 1 must present fresh root PID, fresh actor IDs and PIDs, fresh disjoint ports, a fresh
boot directory, and (cross-node) fresh hard placement — then pass the identical workload and a
fully graceful shutdown. Note: Ray can assign actors to pre-spawned idle workers, so epoch-1
PIDs may be numerically lower than epoch-0 PIDs; freshness is proven by disjoint PID/actor-ID
sets, not PID ordering.

## 8. Workload and bit-exact oracle

One exp68 vocabulary-sharded top-k case (`cross_both`: V=64, split=32, k=6, seed=1), executed
in **both coordinator directions** over the real HPX action path, checked bit-exactly against
the exp68 oracle (imported in place, never copied): exact token IDs, exact ordering, exact
float32 bit patterns, peer PID/locality/hostname witnesses, HPX composition witnesses, and no
application work on the root. Synthetic LLM-shaped work; no real inference.

## 9. In-process hosting proof

For every actor in both epochs: extension PID == Ray worker PID == `os.getpid()`, and zero HPX
child processes (the exp66/67 identity), now demonstrated under failure injection.

## 10. Hard cross-node placement proof

`NodeAffinitySchedulingStrategy(soft=False)` for both actors against resolved Ray node IDs;
cluster attestation (Slurm job identity, two distinct nodes, resolved node IDs); per-epoch
placement freshness; subnet-bound parcelport endpoints; victim-on-remote-node enforced. The
selftests prove same-node placement, soft placement, off-subnet endpoints, and a head-node
victim would each have been rejected. The value oracle alone is never treated as placement
proof.

## 11. Local results

Three accepted local live runs (macOS, loopback), all `pass` with post-loss category
`membership_stale`: `20260717T230159Z`, `20260717T230329Z` (increment 2), and
`20260718T221930Z` (increment 3, experiment-scoped orphan gates). Curated into
`_exp70_slice1_runs/curated_local_evidence/curated_local_aggregate.json` (sha256
`7c6e39083a235c0b512fcfa0f6caf5a1837f43d7a17f62abe1b5e6a7e2af3393`). A fourth local run
(`20260718T223027Z`) validated the `--exp68-build-dir` correction before the hardware phase;
selftests pass 60/60.

## 12. Rostam results

Slurm job **173489** (partition `medusa`, `-N 2 --exclusive`, medusa00 + medusa01, 2026-07-19):
selftest 60/60 inside the allocation, then two fresh cross-node runs —
`crossnode_173489_20260719T003244Z` and `crossnode_173489_20260719T003302Z` — both
`overall=pass`, all 19 gate groups true, post-loss `membership_stale` (survivor membership 3
within a 1–2 ms bounded probe against a 12 s bound), victim observed as Ray `ActorDiedError`.
Environment: Python 3.12.3, Ray 2.55.1, HPX `V2.0.0` Git `20bc3d4bf3` (the verified
fixed-install identity, gated by the launcher before running).

## 13. Artifact copy-back and hash verification

The complete Rostam result set was copied to
`_exp70_slice1_runs/rostam_copyback_20260719T053456Z/` (both raw run directories, per-epoch
root markers/logs, Ray head/worker logs, the Slurm job log, and
`hardware_evidence_manifest_173489.json` + `.sha256`). All **25/25** artifacts verified by
SHA-256 against the manifest (self-hash
`953494974e44f55c277977c90755b048f8682fbd12cd8b2e9dba0d81838060c0`); both run directories
complete. Analysis was performed only after this verification. The run area is ignored by git;
the tracked summary lives in `slice1_curated_evidence.json` beside this README.

## 14. Supported claim

> In a two-node Rostam experiment, unexpected loss of one remotely placed actor-hosted HPX
> locality caused the RayX supervisor to classify the complete island as failed, discard the
> old root and surviving connector, construct a fresh cross-node island, and verify the same
> deterministic distributed HPX workload on the replacement.

## 15. Explicit non-claims

The result does **not** demonstrate: HPX-native heartbeat; HPX-native loss notification;
authoritative failure detection; transparent HPX recovery; partial-island continuation;
application-state restoration; performance improvement; automatic AGAS repair. Slice 1 does
not rely on partial-island continuation — it deliberately replaces the whole island after
classification. Because no application work is attempted on the poisoned island after
classification, Slice 1 cannot conclude whether HPX could have continued; it proves RayX does
not rely on such continuation.

Root sampler nuance (recorded honestly): the work-free root's ~200 ms membership sampler did
not always observe the short three-member interval (`root.final` sometimes records
`max_membership=2`). Island readiness is therefore **not** gated on the root sampler; it is
proven by actor-side membership 3, distinct connector locality identities, bidirectional
remote actions, peer PID/locality/hostname witnesses, and bit-exact results.

## 16. Remaining coverage

* Only one node pair (medusa00/medusa01) is tested; other pairs/subnets are not.
* The `--victim a` asymmetry (losing the actor co-located with root and Ray head) is untested
  — optional Slice 1 robustness work, not part of Slice 2.
* The post-loss membership category is timing-dependent and observational; a different
  category on other hardware would be a recorded observation, not a failure.

## 17. Relationship to the HPX lifecycle feature request

Slice 1 supplies the supervision-side evidence for the planned upstream HPX
locality-lifecycle discussion: a supervisor outside HPX can already classify loss and restart
a whole island, but only via external observation (Ray actor death + external witnesses),
because current public HPX connect-mode APIs expose no native lifecycle/loss surface — the gap
isolated by exp63 and reduced to the two-process reproducer in
[`../upstream_reproducer/`](../upstream_reproducer/README.md) (Slice 0). The Slice 1 result
does not assume, exercise, or test any proposed native API; it defines what a native mechanism
would have to improve upon (see also Slice 0's case 2, the external-lifecycle workaround).

## Files

* `run_slice1.py` — instrument: selftest (60 checks), local two-epoch phase, curation mode,
  Rostam cross-node phase (sha256
  `7be9f757df3449dca37ccec9b924f045efb57163150681fcc8aeaf09ebffa7d4`).
* `exp70_slice1_crossnode.sbatch` — validated two-node launcher (sha256
  `a8beeec876498794a094585106055cc525cd1af3ced71549e652e7c9d36db0fe`).
* `slice1_curated_evidence.json` — tracked curated evidence summary (runs, gates, hashes).
* `_exp70_slice1_runs/` — ignored raw run/copy-back area (see `.gitignore`).
