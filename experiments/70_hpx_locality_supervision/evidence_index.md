# exp70 evidence index

One record per slice. All paths are repository-relative. Raw run and copy-back directories are
**untracked by design** (they remain on disk locally); the tracked evidence is the curated JSON
beside each slice README.

Overview: [`README.md`](README.md) · gaps:
[`native_backend_gap_matrix.md`](native_backend_gap_matrix.md) · HPX-maintainer view:
[`upstream_acceptance_contract.md`](upstream_acceptance_contract.md)

---

## Slice 0 — reduced lifecycle gap and external workaround

| | |
|---|---|
| **Purpose** | Reduce the fixed-connector-lifetime failure to two processes, and demonstrate the external completion/liveness workaround |
| **Sources** | `upstream_reproducer/root.cpp`, `connector.cpp`, `common.hpp`, `CMakeLists.txt` (tracked, public) |
| **Runner** | `upstream_reproducer/run_case.sh` |
| **Cluster runner** | `_exp70_rostam_runs/exp70_slice0_runner.sh` (Linux loopback confirmation) |
| **Topology** | Two processes, loopback: one root + one connector. HPX-only, no Ray, no Python |
| **Demonstration timings** | 3 s former serving window · 6 s idle interval · 15 s external connector deadman (values are demonstration timings, **not defaults**) |
| **Witness** | `root.alive`, **dispatch-driven** — bumped immediately before a root dispatch. **Not periodic**, not an HPX-native heartbeat |
| **Accepted runs** | macOS/libc++ local; Linux/libstdc++ confirmation on Rostam (both cases pass all gates) |
| **Curated evidence** | none separate — `upstream_reproducer/README.md` is the tracked record |
| **Raw artifacts (ignored)** | `_exp70_rostam_runs/20260716T220724Z_slice0_linux_loopback/` (plus a retained `…220424Z` provenance run) |
| **Hash verification** | per-run `sha256sums.txt` + `source_sha256sums.txt` in the run directory |
| **Key finding** | The failure is the missing "no further work will be sent" fact, not the lifetime value. Two-platform signature table: identical type/code/category/site; only `what()` text differs (libc++ vs libstdc++ presentation) |
| **Supported claim** | A connector with a fixed serving window departs before valid work arrives; an explicit external completion/liveness contract removes the guess |
| **Limitations** | Loopback only; single connector; no Ray; no multi-node claim; no performance claim |
| **Upstream** | HPX **#7384** discovered here during stale-target dispatch — posted, fixed, regression-test PR approved. Concerns error reporting, **not** lifecycle supervision |

---

## Slice 1 — Ray-supervised cross-node whole-island replacement

| | |
|---|---|
| **Purpose** | Prove Ray can discard and replace a complete island after unexpected loss of one actor-hosted locality |
| **Runner** | `slice1_actor_hosted_island_restart/run_slice1.py` |
| **Launcher** | `slice1_actor_hosted_island_restart/exp70_slice1_crossnode.sbatch` |
| **Selftest** | 60/60 |
| **Accepted local runs** | 3 |
| **Rostam job** | 173489 |
| **Node pair** | medusa00 (10.42.5.30) / medusa01 (10.42.5.31) |
| **Hardware runs** | `20260719T003244Z`, `20260719T003302Z` — both pass |
| **Curated evidence** | `slice1_actor_hosted_island_restart/slice1_curated_evidence.json` |
| **Raw copy-back (ignored)** | `slice1_actor_hosted_island_restart/_exp70_slice1_runs/rostam_copyback_20260719T053456Z/` |
| **Hash verification** | 25/25 sha256-verified after copy-back; both run directories complete |
| **Key finding** | The safe recovery boundary is the **whole island** — old root and surviving connector are both discarded. Post-loss membership stayed `membership_stale` (observational, not required) |
| **Supported claim** | *Unexpected loss of one remotely placed actor-hosted HPX locality caused the RayX supervisor to classify the complete island as failed, discard the old root and surviving connector, construct a fresh cross-node island, and verify the same deterministic distributed HPX workload on the replacement.* |
| **Limitations** | Whole-island replacement only — no partial-island continuation, no state restoration, no HPX-native detection, no performance claim |

---

## Slice 2A — explicit completion through an external backend-neutral contract

| | |
|---|---|
| **Purpose** | Make "no further work will be sent" an explicit, testable lifecycle event |
| **Runner** | `slice2_explicit_completion/run_slice2.py` |
| **Launcher** | `slice2_explicit_completion/exp70_slice2_crossnode.sbatch` |
| **Selftest** | 73/73 (includes 8 cross-node placement/preflight checks) |
| **Accepted local runs** | 3 |
| **Rostam job** | 173796 |
| **Node pair** | medusa11 (10.42.5.41) / medusa12 (10.42.5.42) |
| **Hardware runs** | `20260719T164029Z`, `20260719T164049Z` — both pass, 15 gate groups / 109 checks, zero failures |
| **Curated evidence** | `slice2_explicit_completion/slice2_curated_evidence.json` |
| **Raw copy-back (ignored)** | `slice2_explicit_completion/_exp70_slice2_runs/rostam_copyback_20260719T214109Z/` |
| **Hash verification** | 25/26 exact, 0 missing. The 26th is the Slurm job log, which that job's in-job manifest hashed while still appending to it; verified as an exact byte-prefix with matching final remote and local hashes. Superseded by Slice 4's two-stage protocol |
| **Key finding** | Idleness does not end the island's obligation to accept work: connectors stayed available across a 6.0001 s idle interval (> the former 3 s window) and served a distinct second workload |
| **Supported claim** | *In a two-node actor-hosted HPX island, both connectors remained available across an idle interval longer than the former fixed serving window, accepted later valid distributed work, and departed cleanly only after explicit completion was published through the external backend.* |
| **Limitations** | The post-completion fence is an application contract gate — **not** a claim that HPX blocks parcels after completion. No native completion, no heartbeat, no loss detection, no performance claim |

---

## Slice 3A — classified connector departure vs unexpected loss

| | |
|---|---|
| **Purpose** | Classify a connector's departure as graceful vs lost from observable evidence alone |
| **Runner** | `slice3_connector_loss_event/run_slice3.py` |
| **Launcher** | `slice3_connector_loss_event/exp70_slice3_crossnode.sbatch` |
| **Selftest** | 96/96 |
| **Accepted local runs** | 2 |
| **Rostam job** | 173797 |
| **Node pair** | medusa06 (10.42.5.36) / medusa07 (10.42.5.37) |
| **Hardware runs** | `20260719T170156Z`, `20260719T170216Z` — both pass, 28 gate groups / 176 checks, zero failures |
| **Curated evidence** | `slice3_connector_loss_event/slice3_curated_evidence.json` |
| **Raw copy-back (ignored)** | `slice3_connector_loss_event/_exp70_slice3_runs/rostam_copyback_20260719T220347Z/` |
| **Hash verification** | 35/36 exact, 0 missing; the 36th is the Slurm job log, verified as an exact byte-prefix (same self-reference as Slice 2A) |
| **Key finding** | A **graceful HPX departure is distinct from host-process death** — the gracefully departed connector's host stays answerable (`{"ok": false, "error": "HPX not started"}`), while a lost connector's host raises `ActorDiedError`. Post-loss membership stayed stale, not gated |
| **Supported claim** | *The external connector-lifecycle backend classified unexpected loss of a remotely placed actor-hosted locality distinctly from normal graceful departure and recorded the bounded HPX membership behavior visible at the observation point.* |
| **Limitations** | Classification is supervisor-computed, not runtime-reported. No native departure event, no heartbeat, no eviction, no recovery, no performance claim |

---

## Slice 4A — explicit root completion vs bounded suspected root loss

| | |
|---|---|
| **Purpose** | Distinguish explicit root completion from unexpected loss of the separately supervised work-free root |
| **Runner** | `slice4_root_loss_event/run_slice4.py` |
| **Launcher** | `slice4_root_loss_event/exp70_slice4_crossnode.sbatch` |
| **Selftest** | 115/115 |
| **Accepted local runs** | 2 |
| **Rostam job** | 173798 |
| **Node pair** | medusa00 (10.42.5.30) / medusa01 (10.42.5.31) |
| **Hardware runs** | `20260719T184648Z`, `20260719T184749Z` — both pass, 31 gate groups / 205 checks, zero failures |
| **Curated evidence** | `slice4_root_loss_event/slice4_curated_evidence.json` |
| **Raw copy-back (ignored)** | `slice4_root_loss_event/_exp70_slice4_runs/rostam_copyback_20260719T234917Z/` |
| **Hash verification** | **38/38 exact, 0 mismatched, 0 missing** via a two-stage protocol: an in-job manifest over closed artifacts declaring `post_job_hash_required`, plus a post-job manifest generated after `sbatch --wait` returned. No prefix reasoning anywhere |
| **Bounds** | 5 s suspicion bound against a 0.2 s expected external-witness refresh; observed silence 5.0131 s / 5.0142 s. Pre-bound probe on a healthy refreshing root returned `observation_timeout`, never suspicion |
| **Key finding** | **After unexpected root loss, actor-hosted HPX calls blocked rather than failing promptly. The supervisor required bounded observations to avoid becoming stranded.** Every post-loss actor probe returned `call_timeout`. Not generalized beyond the tested HPX build and topology |
| **Supported claim** | *In a two-node actor-hosted HPX island, the external root-lifecycle backend distinguished explicit root completion from bounded suspicion after unexpected loss of the separately supervised work-free root, while the supervisor discarded the poisoned island.* |
| **Limitations** | The loss verdict is **bounded suspicion, not detection**. No native root event, no heartbeat, no certainty, no recovery, no AGAS repair, no partial-island continuation, no performance claim |

---

## Cross-node evidence summary

| Slice | Job | Node pair | Passing runs |
|---|---|---|---|
| 1 | 173489 | medusa00 / medusa01 | 2 |
| 2A | 173796 | medusa11 / medusa12 | 2 |
| 3A | 173797 | medusa06 / medusa07 | 2 |
| 4A | 173798 | medusa00 / medusa01 | 2 |

Every hardware run used hard Ray node affinity (`NodeAffinitySchedulingStrategy(soft=False)`),
actor-hosted HPX localities **in-process**, a separately supervised work-free root, TCP
parcelport endpoints on the `10.42.5.` subnet, bit-exact deterministic workloads verified
against an independent oracle, artifact copy-back **before** analysis, and hash verification
after copy-back.

All four slices reused the same prebuilt exp68 artifacts in place, with identical hashes across
jobs 173489 / 173796 / 173797 / 173798, against the same fixed HPX install
(`20bc3d4bf3068383edcb63be13f22e9ff95842fa`), gated by each launcher before any run.
