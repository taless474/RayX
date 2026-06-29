# exp53 — Ray-supervised HPX island restart under the whole-island-fatal policy

**Status:** pass (single-node, loopback TCP, controlled supervisor kill/restart only).
**Predecessors:** exp49 graceful connect-mode lifecycle; exp50 ungraceful connector-loss
characterization; exp51 stale-locality shutdown boundary (bounded finalize / local cleanup do **not**
rescue the poisoned root; whole-island external restart is the safe recovery boundary); exp52 the
first Ray-orchestrated clean bootstrap. exp53 is the first experiment that makes Ray's supervision
role **load-bearing**: it exercises the exp51 whole-island-fatal policy end to end.

## What this is (and is not)

**exp53 exercises the Ray-side whole-island-fatal policy. Island #1 and island #2 are two
independent HPX runtimes, not one runtime recovering.** Ray acts as the durable supervisor / restart
plane; HPX remains the execution / data plane inside each island. The poisoned island is
**discarded, not repaired**. This validates **supervisor process-lifecycle policy, not HPX fault
tolerance.**

From HPX's point of view there is **no new mechanism**: island #1's failure is exp50's mid-flight
connector loss, and island #2's clean bootstrap is exp52. exp53 only proves, at the Ray level, that
a durable supervisor can kill a poisoned island and launch a fresh clean one. Specifically:

- The **failure root never finalizes** — it is SIGKILLed mid-idle (signal 9), so it never enters the
  exp51 collective-shutdown hang.
- **Poisoning is witnessed before the kill** — the supervisor waits for the root to record that its
  long-action future did not return (the mid-flight loss landed) before marking the island poisoned.
- **Island #2 uses fresh ports** (and a fresh rendezvous dir): a SIGKILLed parcelport/root may leave
  OS-level bind / TIME_WAIT / cleanup artifacts, so fresh ports keep a genuine restart failure from
  being confused with a port-reuse artifact.
- The HPX action travels **HPX → HPX over the parcelport**; Ray carries only bootstrap metadata, and
  **no HPX property is re-validated beyond the exp50/exp52 mechanisms**.

## Core question

Can a durable Ray supervisor implement the whole-island-fatal policy — after an ungraceful HPX
connector loss, discard the entire poisoned island (root included) and prove a fresh island works —
without any in-place repair or AGAS stale-locality cleanup?

## Design

A local `ray.init()` driver and **one durable `@ray.remote IslandSupervisor`** that owns both HPX
child processes (and process groups) of each island, keyed by `island_id`. The **same supervisor**
launches island #1 and island #2 sequentially — proving the Ray plane persists across HPX-island
death. The supervisor also owns connector-loss **injection** and whole-island **kill/reap**.

Supervisor methods (all bounded; metadata/status returns only): `launch_island(meta, mode)`,
`inject_connector_loss(island_id, timeout)`, `wait_loss_witness(island_id, timeout)`,
`mark_poisoned(island_id)`, `kill_island(island_id)`, `read_result`, `wait_exit`, `shutdown()`.

One thin standalone HPX binary (`island_restart_spike`, isolated CMake with a baked RPATH, **not**
wired into `_rayx`), reusing exp50 (long action + victim) and exp52 (clean path):

- **`f_root --island-mode failure`:** admit the victim connector, dispatch the long `dist_sleep_probe`
  with a bounded `wait_for`, **classify the loss** (the witness), write `failure_root.json`, then
  **idle inside `hpx_main` until SIGKILL** — no `finalize`, no `disconnect`, no normal return in the
  expected path. The 120 s idle cap is a safety guard; if it elapses the binary writes an anomaly
  marker, and the run is **not** a clean pass.
- **`f_root --island-mode clean`:** exp52 clean path — serve one closed-`int64` `dist_probe`, write
  `served1.ok`, `wait_id_absent` (graceful-leave gate), write `clean_root.json`, `hpx::finalize()`.
- **`f_connect --connector-kind victim`:** join, idle, **never disconnect** — expects SIGKILL.
- **`f_connect --connector-kind clean`:** exp52 graceful `post(disconnect)+stop`.

Closed-`int64` `dist_probe` + `dist_sleep_probe` (writes `action_started` as its first statement so
the connector can be SIGKILLed while the body provably runs on it); no managed `hpx::id_type`.

### Launch hygiene (exp52, every HPX child)

Self-locating **RPATH** (launched with the loader env **scrubbed** — `DYLD_LIBRARY_PATH`,
`DYLD_FALLBACK_LIBRARY_PATH`, `LD_LIBRARY_PATH` removed — to prove self-location);
**`--hpx:ignore-batch-env`**; **`--hpx:bind=none`**; root `--hpx:threads=2` / connector
`--hpx:threads=1`; numeric `127.0.0.1`; **no** `--hpx:localities=N`; absolute path;
`start_new_session=True`.

### Sequence

1. `ray.init()` (local); create one `IslandSupervisor`.
2. Launch island #1 (failure mode, victim connector) on fresh ports/dir; wait `root.ready` + joined.
3. Wait `action_started`; supervisor **SIGKILLs the connector process group** (mid-flight, exp50 A).
4. Supervisor **waits for `failure_root.json`** and confirms `loss_observed_by_root=true` — **witness
   before kill**.
5. `mark_poisoned(1)` (only after the witness).
6. `kill_island(1)`: SIGKILL connector (idempotent) then **root**; bounded reap of all children; **no
   finalize**.
7. Launch island #2 (clean mode) on **fresh** ports/dir (same supervisor).
8. Island #2 runs one closed-`int64` action; connector `post(disconnect)+stop`; root
   `wait_id_absent` → `hpx::finalize()` clean.
9. `shutdown()` sweep; post-run `pgrep` orphan check for `island_restart_spike`.

## Result (this machine: AppleClang 17, HPX 1.11 networking build; Ray 2.55.1, local; loopback TCP)

| check | value |
|---|---|
| island #1 root started / connector joined | yes / yes |
| island #1 `action_started` seen / connector SIGKILLed | yes / yes |
| island #1 long-action outcome (loss witness) | **`timed_out`** (exp50 Case A) |
| island #1 `loss_observed_by_root` | **yes** (witnessed before kill) |
| island #1 marked poisoned (after witness) | yes |
| island #1 root killed by supervisor — exit signal | **9 (SIGKILL)** |
| island #1 failure root **never finalized** / idle-cap elapsed | **yes** / no |
| island #1 all children reaped | yes |
| island #2 root started / connector joined | yes / yes |
| island #2 root / connector locality | 0 / 1 (genuine cross-locality) |
| island #2 action proved remote | **yes** |
| island #2 graceful teardown clean / root finalized clean | yes / yes |
| `whole_island_restart_succeeded` | **yes** |
| `supervisor_survived_island_death` | **yes** |
| island #2 used fresh ports / `ports_reused` | **yes** / no (disjoint from island #1) |
| no orphan `island_restart_spike` processes | **yes** |
| Ray carried bootstrap metadata only / action result via Ray | yes / **no** |
| binary self-locating RPATH / `--hpx:ignore-batch-env` on all children | yes / yes |

`overall = pass`. **Stable across 3 consecutive runs** — identical on every gate, and a final
`pgrep` sweep confirmed no leftover HPX processes.

## Interpretation

What this **supports:** a durable Ray supervisor can implement the exp51 whole-island-fatal policy on
one node — inject a mid-flight connector loss, **witness** that the root observed it (long-action
future `timed_out`), mark the island poisoned, **SIGKILL the whole island including the root (no
finalize, no AGAS cleanup, no root preservation)**, reap all children, and then launch a **fresh,
independent** HPX island on fresh ports that bootstraps, proves a cross-locality action, and tears
down cleanly. The supervisor itself never dies — the Ray plane is durable while the HPX island is
disposable.

What remains **out of scope / not shown:** this is a **single, controlled supervisor kill/restart
cycle**. It does **not** test Ray *actor* failure (the supervisor was healthy throughout), multi-node,
repeated/automatic restart under real crashes, or any detection mechanism beyond the harness watching
a marker file. The HPX poisoning itself is established by exp50/51 and **witnessed** here, not
re-derived.

What must **not** be claimed: this is **not HPX fault tolerance**, **not** in-place recovery, **not**
crash recovery, **not** a new HPX result, **not** multi-node, **not** general fabric, and carries
**no** performance/latency claim. Island #2 is a **fresh independent HPX runtime, not repaired island
#1**. Ray is the bootstrap/supervision/restart plane only; HPX is the execution/data plane inside each
island.

## Roadmap impact

**Classification: Roadmap strengthened (supervision-policy leg).** The future distributed-fabric
direction now has its supervision plane demonstrated end to end: Ray can discard a poisoned HPX island
and restart a fresh one under the exp51 policy, with the failure-root finalize-hang structurally
avoided (killed mid-idle) and the poisoning witnessed rather than assumed.

- **In-process HPX-inside-Ray-actors track:** unaffected — this is a distributed-island lifecycle
  property, not an in-process one.
- **Future distributed-fabric direction:** strengthened but still gated. The clean bootstrap (exp52)
  and the whole-island-fatal restart policy (exp53) are now both demonstrated on one node. This does
  **not** pull a fabric/performance/multi-node claim forward; it establishes that Ray's
  supervision/restart role is mechanically sound for the validated single-island mechanism.

## Next recommended step

One Ray-supervised experiment that makes the supervisor **detect** the loss rather than be told where
to look: have the `IslandSupervisor` derive "island poisoned" from a **bounded liveness signal it
owns** (e.g. the connector child's process exit plus a bounded root-progress timeout) instead of
watching a hand-placed `action_started` marker, then apply the same kill/restart. That moves exp53's
*scripted* policy toward a *supervisor-owned* failure-detection policy — still single-node, Ray-free
of any data path, no performance claim — and is the smallest step that makes the supervision plane
autonomous without yet touching multi-node or fabric.

## Claim fence

Single-node · loopback TCP · closed-`int64` action only · Ray = bootstrap/supervision/restart plane
only · HPX = execution/data plane inside each island · **whole-island-fatal policy exercised** ·
**not HPX fault tolerance** · not in-place recovery · no AGAS stale-locality repair · no Ray
actor-failure-recovery claim beyond this controlled supervisor kill/restart · **island #2 is a fresh
independent HPX runtime, not repaired island #1** · no multi-node · no general fabric · no
performance/speedup/throughput/latency · no production/public API · no endpoint seam · no Ray
replacement · no "HPX faster than Ray" · no "RayX makes Ray faster" · future distributed-fabric
direction remains gated.

## Reproduce

```
cmake -S experiments/53_ray_supervised_island_restart \
      -B experiments/53_ray_supervised_island_restart/build \
      -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
cmake --build experiments/53_ray_supervised_island_restart/build
python experiments/53_ray_supervised_island_restart/run_ray_supervised_island_restart.py
```

`build/` is gitignored. The curated `aggregate.json` is tracked; raw per-run logs/bootdirs stay under
per-run temp dirs and are not tracked. **Not part of normal CI** — Ray-capable smoke tier only; it
skips cleanly (`overall="skip"`, exit 0) when Ray or the built binary is unavailable (no HPX source
build / no Ray-supervision drivers in normal CI).
