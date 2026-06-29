# exp57 Slice 0 — attribution correction

**Status:** supersedes the earlier Slice 0 readiness-duration label. Interpretation correction only; no
mechanism result is retracted.

This note records why the ~30 s two-node *readiness* duration observed in exp57 Slice 0 (the Ray-free
settle-attribution diagnostic) is **not** what its first label said, and what the correct, evidenced
attribution is. It is a control-path / orchestrator artifact, not an HPX, AGAS, network, or fabric
property.

---

## 1. Supersession

- The earlier classification **`slurm_step_launch_latency` is withdrawn / superseded**. It must not be
  used as the final Slice 0 explanation.
- It rested on a *login-shell* probe: `srun -N1 -n1 --nodelist=<node> bash -lc 'date +%s.%N; hostname'`,
  which measured ~25 s on medusa00.
- That probe is **off the real HPX launch path**: the exp57 binary is `srun`-exec'd **directly**
  (`srun … two_node_island_spike --role … `), not via `bash -l`. The ~25 s was login-shell / profile
  (`/etc/profile.d/*`, module init) startup on medusa00, which the direct-exec launch never pays.

## 2. Final attribution

- **`nfs_negative_dentry_or_attribute_cache`.**
- The ~30 s readiness duration was a **supervisor-side shared-FS marker-polling artifact**: the
  orchestrator polled for a not-yet-existing marker under `/work` using plain `os.path.exists`. NFS
  **negative-dentry / directory-attribute caching** then hid the marker from the supervisor even after
  it had been created + `fsync`'d + closed remotely, until the client attribute cache (~`acdirmax`)
  expired.
- **Parent-directory revalidation via `os.listdir(parent)` defeats the artifact** (a fresh directory
  lookup invalidates the cached negative result). This is exactly what the hardened reader
  (`read_json_eventually`) does, which is why Slice 0's *final reparse* recovered markers that the
  *live gating* loop (plain `os.path.exists`, no revalidation) missed — consistent with the Slice 0
  flag `marker_read_false_negative_suspected=true`.

## 3. Evidence chain (all Ray-free, supervisor-wall timing)

| candidate | probe | result | verdict |
|---|---|---|---|
| DNS / reverse-DNS | `getent hosts <name|ip>` | ~3 ms | ruled out |
| Slurm non-overlapping steps | `srun true` vs `srun --overlap true` | ~90 ms; `--overlap` no help | ruled out |
| Node prolog / cgroup | bare `srun true` | ~90 ms | ruled out as the ~30 s source |
| Login-shell startup | `srun … bash -lc 'date; hostname'` | ~25 s on medusa00 | real, but **off launch path** |
| Direct binary exec / loader | `srun … two_node_island_spike --hpx:version` | ~235–268 ms | ruled out |
| Raw marker write→visible | write marker, poll **after** write | ~110–127 ms | ruled out |
| **Negative pre-existence polling** | poll **before** write, then write | **see below** | **confirmed** |

Negative pre-existence polling (both nodes, decisive):

- **control** (poll after write, revalidating): ~117–119 ms, attempts=1.
- **negative robust** (8 s pre-poll, `os.listdir(parent)` revalidation): ~111–113 ms, `failed_after=0`.
- **negative simple** (8 s pre-poll, plain `os.path.exists` — replicates Slice 0's live gating):
  ~22043–22053 ms, `failed_after=219`.
- `localized_cause=nfs_negative_dentry_or_attribute_cache`; `negative_poll_delay_seen=true`;
  `revalidation_appears_to_help=true`; `overall=localized`.
- The delay is **not node-specific** (both nodes ~22 s) → a client-side (supervisor) cache property,
  not a node property. With an 8 s pre-poll the post-write delay was ~22 s; Slice 0 polled continuously
  from launch, so the full negative-cache TTL (~`acdirmax` ≈ 30 s) elapsed → the observed ~30 s band.

## 4. Mechanism result preserved

The HPX two-node island mechanism remains **correct and unchanged**. Slice 0 still demonstrated:

- reached two localities;
- closed-int64 oracle match;
- remote locality id differs;
- host / IP differs;
- clean connector disconnect;
- bounded root finalize;
- no orphans on either node.

Only the *interpretation of the readiness duration* changed. No mechanism result is retracted.

## 5. Claim hygiene

- Do **not** call the ~30 s HPX/AGAS settle.
- Do **not** call it network latency.
- Do **not** call it fabric performance.
- Do **not** call it general filesystem performance.
- It is a **control-path / orchestrator marker-polling artifact** in this launch model only.
- Do **not** reuse this readiness duration for restart / detector timing (exp55's single-node
  calibration does not transfer, and this duration is an artifact, not a mechanism cost).

---

## 6. Revised Slice A plan deltas (plan of record; implementation NOT yet approved)

Slice A (the Ray/Slurm-supervised clean-path two-node island) must:

- issue the root and connector `srun` steps **concurrently / near-concurrently**;
- **not** gate the connector launch on `root.ready`;
- use a **connector-side AGAS TCP pre-probe** (direct TCP readiness to `A_ip:agas_port`) instead of
  shared-FS readiness gating;
- use the **revalidating** marker readers (`read_json_eventually` / `exists_eventually`) for **all**
  critical marker waits;
- **forbid plain `os.path.exists` polling** on not-yet-existing critical markers anywhere in the path
  (this was the Slice 0 defect — the *live gating* loop, not just the final reparse, must revalidate);
- **do not await connector disconnect before stop** — use the proven HPX idiom
  `hpx::post([]{ hpx::disconnect(); }); hpx::stop();` and validate graceful leave from the **root-side
  `observed_connector_leave=true`** (AGAS-level / cross-node), not a connector-local completion flag.
  (A Rostam settle-diag run proved that awaiting `hpx::disconnect()` before `hpx::stop()` **deadlocks**:
  the two are coupled — the posted disconnect needs shutdown progress driven by `hpx::stop()` on the
  main thread, so blocking main to wait for disconnect completion is a circular wait. The exp50/51
  stale-locality class is about **abnormal / ungraceful** loss and root-side stale state, **not** the
  graceful connector self-disconnect path, which `post(disconnect); stop()` handles correctly.)
- keep the **Ray child-env scrub** as required (pre-`import ray` env snapshot; preserve loader/module
  vars; `ldd_ok` gate);
- **not** use `--overlap` as a required fix — A.0 showed it gives ~0 benefit;
- keep **failure / restart deferred to exp58** (clean path only).

Corrected rationale: direct binary `srun` exec is fast (~235–268 ms), so serialized placement was never
the real cost. The real ~30 s was **negative-cache-prone shared-FS readiness gating** (pre-polling a
not-yet-existing marker with plain `exists()`). Slice A avoids it by concurrent issuance + a direct TCP
AGAS readiness probe inside the connector, with revalidating reads for any remaining marker waits.

## 7. Planned aggregate / write-up fields for future Slice A

To be recorded by the future `--phase run` aggregate (also enforced as structural pass-gates so the
Slice 0 defect cannot silently reappear):

- `slice0_settle_delay_classification = "nfs_negative_dentry_or_attribute_cache"`
- `slice0_attribution_superseded_label = "slurm_step_launch_latency"`
- `marker_waits_revalidating = true`
- `plain_exists_critical_waits = false`
- `connector_uses_agas_tcp_preprobe = true`
- `shared_fs_readiness_gate_on_connector_launch = false`
