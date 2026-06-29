# exp54 — autonomous Ray-side poisoned-island detection

**Status:** pass (single-node, loopback TCP, controlled detection cycle only).
**Predecessors:** exp49 graceful lifecycle; exp50 ungraceful connector-loss; exp51 stale-locality
shutdown boundary (no rescue; whole-island external restart is the safe recovery boundary); exp52
clean Ray bootstrap; exp53 Ray-supervised whole-island restart (but with a **scripted**
`action_started` trigger and a supervisor-caused connector kill). exp54 removes both crutches: the
supervisor must **detect** poisoning from signals it owns, and the connector death must be
**observed, not caused**.

## What this is (and is not)

**exp54 is a detection-logic experiment.** The Ray supervisor **cannot query HPX for authoritative
locality health** — exp51 found no such API — so it infers island poisoning from **OS process
liveness + bounded progress/completion markers**. This experiment follows directly from exp51's
negative result. From HPX's point of view there is **no new mechanism**: the failure path is exp50's
connector loss and the clean path is exp52/exp53. **No HPX property is re-validated.**

Two corrections over exp53 make the "autonomous" claim honest:

1. **The connector death is observed, not caused.** Island #1's connector **self-crashes**
   (`std::abort()`) a fixed delay after join, so the supervisor observes an **uncaused** death via
   its own `Popen.poll()` — it is not confirming a death it initiated. (On this HPX build
   `std::abort()` surfaces as **SIGILL / signal 4**, not SIGABRT(6): HPX's installed abort/terminate
   handler traps. The classifier accepts the whole ungraceful-crash signal family, excluding the
   supervisor's own SIGKILL.) Supervisor SIGKILL is used **only** for cleanup/reap **after**
   classification.
2. **A uniform detector predicate, run for both a clean control and the failure island.** The
   predicate is implemented **once** and evaluated for island #0 (clean control) and island #1
   (failure) on the **same bounded timeline** — there is no per-island "failure detector" branch.

The scripted `action_started` marker is **not** a classifier input; it is recorded only as the
diagnostic `action_was_in_flight`.

## Core question

Can the Ray supervisor derive "this HPX island is poisoned" from liveness/progress signals it owns —
an **uncaused** observed connector crash plus bounded root non-completion — without a scripted
trigger, and **without false-positiving on a healthy island**?

## Uniform detector predicate

For **any** island, classify `poisoned` iff all hold:

```
connector_not_alive  AND  connector_clean_disconnect_absent
  AND  (clean_completion_within_T == false)  AND  root_not_cleanly_exited
```

Signals, ranked by ownership: `connector.poll()` and `root.poll()` are **fully** supervisor-owned
(its own `Popen` handles); the **absence** of a clean-completion marker (`clean_root.json`, written
by a root **only on success**) within `root_progress_timeout` is a supervisor-owned **progress
policy**; the absence of a clean-disconnect marker confirms the death was ungraceful. The predicate
**short-circuits**: it resolves to `poisoned` as soon as the connector is dead with no clean leave
(fast, ~connector-death latency), or to `clean_complete` as soon as the success marker appears — so
failure detection is decisive in ~2 s while a healthy island is given the full window.

The honest emphasis: the load-bearing autonomous signals are **(a) the observed uncaused connector
crash** and **(b) root-still-alive-without-completion**; the progress timeout is the bound that makes
(b) decisive. The clean-completion-absence is near-tautological given a dead connector and is
corroborating, not independent.

## Design

A local `ray.init()` driver and one durable `@ray.remote IslandSupervisor` owning all child `Popen`s.
One thin standalone HPX binary (`autonomous_poison_spike`, isolated CMake with baked RPATH, **not**
wired into `_rayx`), adapted from exp53: root `--island-mode failure|clean`, `f_connect
--connector-kind self_crash|clean` (`victim` retained as a non-primary second arm). Failure root
dispatches the long action then **idles in `hpx_main`, never finalizes** (exp53 correction — keeps
the exp51 finalize-hang out, so "root alive" is a clean idle) and writes **no** clean-completion
marker. Closed-`int64` actions; no managed ids.

**Sequence:** island #0 clean control (predicate must **not** fire, same timeline) → island #1
failure (self-crash connector; predicate fires autonomously) → kill/reap island #1 → island #2 clean
restart proof on **fresh disjoint ports/dir**. Launch hygiene per exp52/53 (self-locating RPATH with
scrubbed loader env, `--hpx:ignore-batch-env`, `--hpx:bind=none`, root `threads=2`/connector
`threads=1`, numeric `127.0.0.1`, no `--hpx:localities=N`).

## Result (this machine: AppleClang 17, HPX 1.11 networking build; Ray 2.55.1, local; loopback TCP)

| check | value |
|---|---|
| **control island #0** root started / connector joined | yes / yes |
| control evaluated on same timeline | yes |
| control clean completion within T / clean disconnect seen | yes / yes |
| control classification / **false positive** | `clean_complete` / **no** |
| **failure island #1** connector kind | `self_crash` |
| connector exit signal (uncaused self-crash) | **4 (SIGILL — abort-trap on this build)** |
| `connector_self_crashed` / `connector_death_caused_by_supervisor` | **yes** / **no** |
| `detection_observed_uncaused_death` | **yes** |
| connector not alive / clean disconnect absent | yes / yes |
| clean completion within T / root not cleanly exited / root alive | **no** / yes / yes |
| `poison_detected_by_supervisor` / `poison_detection_used_scripted_action_marker` | **yes** / **no** |
| `action_was_in_flight` (diagnostic only) | yes |
| island #1 classification | **`poisoned`** |
| island #1 root killed by supervisor / never finalized / all reaped | yes (sig 9) / yes / yes |
| **restart island #2** fresh ports / `ports_reused` | **yes** / no |
| island #2 root / connector locality | 0 / 1 (genuine cross-locality) |
| island #2 action proved remote / teardown clean / finalized clean | yes / yes / yes |
| `whole_island_restart_succeeded` / no orphans | **yes** / **yes** |
| Ray carried metadata only / action result via Ray | yes / **no** |

`overall = pass`. **Stable across 3 consecutive runs** — identical on every gate; a final `pgrep`
sweep confirmed no leftover HPX processes. The **same** predicate classified the clean control
`clean_complete` and the failure island `poisoned`, demonstrating discrimination (no false positive
on a healthy island).

## Interpretation

What this **supports:** a durable Ray supervisor can autonomously classify an HPX island as poisoned
from signals it owns — an **uncaused** observed connector crash (the connector self-`abort()`ed; the
supervisor did not kill it) plus bounded root non-completion and no clean leave — and the **same
uniform predicate** correctly leaves a healthy island unflagged (`clean_complete`). It then kills and
replaces the poisoned island with a fresh independent one that serves a cross-locality action and
finalizes cleanly. This is the autonomous-detection step exp53's scripted trigger lacked.

What remains **out of scope / not shown:** a single controlled detection cycle on one node. The
crash is still injected at a fixed delay (just not *announced* by the action); there is no detection
under real concurrent load, no Ray-actor failure, no multi-node, no repeated/automatic detection at
scale. The progress timeout is tuned above a healthy island's ~15 s completion — a real deployment
would need to size it against its own completion distribution (a healthy-but-slow island with a
too-short timeout would risk a false positive; the control arm is what guards this).

What must **not** be claimed: this is **not HPX fault tolerance**, **not** in-place recovery, **not**
crash detection at scale, **not** a new HPX result, **not** multi-node, **not** general fabric, and
carries **no** performance/latency claim. Island #2 is a **fresh independent HPX runtime, not
repaired island #1**. Ray is the bootstrap/supervision/restart/detection plane only; HPX is the
execution/data plane inside each island.

## Roadmap impact

**Classification: Roadmap strengthened (autonomous-supervision leg).** exp53 demonstrated the
whole-island-fatal *policy* with a scripted trigger; exp54 makes the supervisor's poisoning
classification **autonomous** (uncaused observed crash + bounded progress) and adds a **false-positive
control** proving the detector discriminates.

- **In-process HPX-inside-Ray-actors track:** unaffected — distributed-island lifecycle property.
- **Future distributed-fabric direction:** strengthened but still gated. The supervision plane can
  now bootstrap (exp52), restart under policy (exp53), and **autonomously detect** poisoning (exp54),
  all single-node. This does **not** pull a fabric/performance/multi-node claim forward.

## Next recommended step

One Ray-supervised experiment that stresses the detector's **discrimination under load / timing**:
run the uniform predicate against a **healthy-but-slow** island (a clean island whose completion
approaches `root_progress_timeout`) alongside the self-crash failure, and characterize the
false-positive margin as the timeout is swept — turning exp54's single control into a **detector
operating-point** characterization (still single-node, no performance claim, no fabric). That is the
smallest step that hardens the detection policy before any multi-node or concurrency work.

## Claim fence

Single-node · loopback TCP · closed-`int64` action only · Ray = bootstrap/supervision/restart/
detection plane only · HPX = execution/data plane inside each island · supervisor-owned
poisoned-island detection from OS process liveness + bounded progress markers · **connector death
observed, not caused by the supervisor, in the primary arm** · **not HPX fault tolerance** · not
in-place recovery · no AGAS stale-locality repair · no Ray actor-failure-recovery claim · island #2
is a fresh independent HPX runtime, not repaired island #1 · no multi-node · no general fabric · no
performance/speedup/throughput/latency · no production/public API · no endpoint seam · no Ray
replacement · no "HPX faster than Ray" · no "RayX makes Ray faster" · future distributed-fabric
direction remains gated.

## Reproduce

```
cmake -S experiments/54_ray_autonomous_poison_detection \
      -B experiments/54_ray_autonomous_poison_detection/build \
      -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
cmake --build experiments/54_ray_autonomous_poison_detection/build
python experiments/54_ray_autonomous_poison_detection/run_ray_autonomous_poison_detection.py
```

`build/` is gitignored. The curated `aggregate.json` is tracked; raw per-run logs/bootdirs stay under
per-run temp dirs and are not tracked. **Not part of normal CI** — Ray-capable smoke tier only; it
skips cleanly (`overall="skip"`, exit 0) when Ray or the built binary is unavailable.
