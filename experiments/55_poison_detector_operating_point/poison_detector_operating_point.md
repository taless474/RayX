# exp55 — Poison-detector clean-marker timeout calibration (+ HPX shutdown-timeout preflight)

> **Status:** orchestrator-side detector calibration. **No new HPX mechanism. No performance claim.**
> This reuses the exp54 `autonomous_poison_spike` binary **unchanged** and **imports the exact exp54
> detector rule** (`evaluate_health_predicate`); the rule is never rederived here.

## What this calibrates (and what the detector actually times)

exp54 built an autonomous Ray-side detector that infers a *poisoned* HPX island from OS-process
liveness plus bounded progress/completion markers, using the uniform predicate

```
POISONED iff  connector_not_alive ∧ connector_clean_disconnect_absent
              ∧ ¬clean_completion_within_T ∧ root_not_cleanly_exited
```

`T = root_progress_timeout`. exp54 noted the detector depends on a timeout operating point: `T` must
exceed a healthy island's completion, or a clean island is misread as poisoned. exp55 characterizes
that sensitivity.

**Load-bearing implementation fact.** In `autonomous_poison_spike.cpp` the clean root writes
`clean_root.json` **before** `hpx::finalize()`, and the clean root dispatches the **instant**
`dist_probe_action` — **not** `dist_sleep_probe`. Therefore, for this binary:

1. **The detector times clean-marker arrival, not distributed shutdown/finalize completion.**
   `clean_root.json` is the marker the predicate races; finalize happens downstream of it.
2. **`time_to_marker_ms` is independent of `sleep_ms`.** Only the *failure* root uses
   `dist_sleep_probe(x, sleep_ms)`, and the failure arm writes **no** marker. So a
   `sleep_ms`-relative timeout floor (the original exp55 plan) is the **wrong model** for this binary.

Consequently the headline is an **absolute** clean-marker timeout floor, plus a small `sleep_ms`
control arm that empirically confirms the marker is `sleep_ms`-independent. `delta_over_sleep_ms` and
`recommended_offset_over_sleep_ms` are deliberately **not** reported — they are meaningless here.

## Method

Reuses exp54's built binary and imports `IslandSupervisor.evaluate_health_predicate` directly (proof:
`aggregate.json → predicate_source` / `predicate_imported_not_copied: true`). Ray = supervision/
detection plane; HPX = execution/data plane inside each island. Single node, loopback TCP, closed
`int64` action only.

- **Phase 0 — HPX shutdown-timeout preflight (framing only).** Drive a *genuinely* poisoned root via
  the idle-cap anomaly path (connector self-crashes **after** it has registered), with and without
  `--hpx:ini=hpx.shutdown_timeout=<T>`. A **baseline control with no timeout** establishes whether the
  poisoned root hangs at all on this path. If it self-exits even without the config, exits cannot be
  attributed to the config → report `inconclusive`.
- **Phase 1 — absolute clean-marker timeout sweep (HEADLINE).** Sweep
  `root_progress_timeout_ms ∈ {10000,14000,15000,16000,18000,20000,25000}`, fixed `crash_delay_ms`,
  `repeats=3`. Per timeout: a clean-control arm (expect `clean_complete`; `poisoned` ⇒ false positive)
  and a self-crash arm (expect `poisoned`), classified with the imported predicate. Clean-arm
  `time_to_marker_ms` and `time_to_finalize_ms` are measured separately, **independent of the per-cell
  timeout** (the orchestrator waits for the marker regardless, to recover the true marker tail).
- **Phase 1b — `sleep_ms` independence control.** Clean arm only, at a safe timeout, over
  `sleep_ms ∈ {2000,8000,14000}`; expect a flat `time_to_marker_ms`.
- **Phase 2 — registration-boundary taxonomy (small, descriptive).** Vary the self-crash timing across
  the AGAS-registration boundary. The reused binary crashes **only after** writing `connect.joined1`,
  so a pre-join crash is **not achievable** without a new connector mode and is reported as
  `not_supported_reused_binary` (no binary change was made).

Classification taxonomy: `clean_correct`, `false_positive`, `poison_correct`, `false_negative`,
`indeterminate`, `late_clean_result_after_poison`.

## Results

<!-- RESULTS:BEGIN -->
Full run: `overall=pass`, predicate imported (`predicate_imported_not_copied=true`), no orphans,
Ray carried bootstrap metadata only. Default axis, `crash_delay_ms=1000`, `repeats=3`.

**Phase 1 — absolute timeout sweep (clean / self-crash verdicts, 3 repeats each):**

| `root_progress_timeout_ms` | clean arm | self-crash arm | cell stable |
|---|---|---|---|
| 10000 | late_clean ×3 | poison_correct ×3 | no |
| 14000 | late_clean ×3 | poison_correct ×3 | no |
| 15000 | late_clean ×3 | poison_correct ×3 | no |
| **16000** | **clean_correct ×3** | poison_correct ×3 | **yes** |
| 18000 | clean_correct ×3 | poison_correct ×3 | yes |
| 20000 | clean_correct ×3 | poison_correct ×3 | yes |
| 25000 | clean_correct ×3 | poison_correct ×3 | yes |

- **`safe_marker_timeout_floor_ms = 16000`**; `recommended_root_progress_timeout_ms = 17309`
  (marker tail + 2000 ms margin). Stable region `[16000, 25000]`.
- **No `false_positive`, no `false_negative`, no `indeterminate` cells.** Below the floor the clean arm
  degrades to `late_clean_result_after_poison` (10000/14000/15000) — i.e. the marker *did* arrive, just
  after the clipped window. Note: a true `false_positive` is structurally unlikely on this binary,
  because the clean connector's `connect.disconnected1` marker guards the poison predicate; the real
  hazard of a too-short timeout is **under-resolution (clipped/late)**, not misclassification as
  poisoned.

**Timing (n=21 clean-arm runs):**
- `time_to_marker_ms`: min 15240.5, p50 15242.8, **max 15309.3** — razor-tight ~15.24–15.31 s.
- `time_to_finalize_ms`: min 10.1, p50 35.4, **max 59.1** — three orders of magnitude smaller than the
  marker, confirming the detector races *marker arrival*, not the shutdown barrier.

**Phase 1b — `sleep_ms` independence (clean marker, ms):** sleep 2000 → 15241–15256; sleep 8000 →
15242–15244; sleep 14000 → 15239–15240. `clean_marker_independent_of_sleep_ms = true` — empirically
flat, as predicted from the clean root using the instant `dist_probe`.

**Phase 0 — preflight:** `inconclusive`. Baseline (no `hpx.shutdown_timeout`) genuinely-poisoned root
self-exited in ~0.2 ms, rc=0 → this idle-cap path does not reproduce the exp51 hang, so exits cannot be
attributed to the config. (See "Phase 0 outcome" below.)

**Phase 2 — registration-boundary taxonomy:** `before_join_marker` = `not_supported_reused_binary`
(the reused connector crashes only after writing `connect.joined1`). The two achievable points
(`immediately_after_join`, `after_action_in_flight`) both classify `poisoned` with the connector
registered; `classification_changes_across_boundary = false`. Within the achievable post-join range,
crash timing does **not** move the verdict — consistent with the predicate being OS-liveness driven.
<!-- RESULTS:END -->

### Headline timing facts (this build / this machine)

- The clean marker is dominated by an HPX **connect-mode AGAS registration-reflection latency**: the
  root's `wait_two` polls `find_all_localities()` until it sees the connector, and on this build that
  takes **~15.2–15.4 s** and is strikingly consistent across runs. `time_to_finalize_ms` is tiny by
  comparison (tens of ms) — confirming finding (1) above: the floor is set by *marker arrival*, not by
  the distributed shutdown barrier.
- The clean-marker false-positive/clipping boundary therefore sits just above ~15.3 s; timeouts below
  it clip the healthy marker (`indeterminate` / `late_clean_result_after_poison`), timeouts above it
  classify `clean_correct`. This is why exp54's default `T=25 s` worked (ample margin).
- `sleep_ms` does **not** move the clean marker (Phase 1b), as predicted from the code: the clean root
  uses the instant `dist_probe`.

### Phase 0 (preflight) outcome

The idle-cap anomaly path does **not** reproduce exp51's `runtime_distributed::wait()` hang on this
build: even a genuinely poisoned root (connector registered, then dead) self-exits cleanly in ~0.2 ms
(rc=0) **without** any `hpx.shutdown_timeout` — because the connector dies several seconds before the
root reaches finalize, leaving HPX time to drop the broken connection. Exits therefore cannot be
attributed to the shutdown-timeout config, so Phase 0 is reported **`inconclusive`** and recommends a
dedicated separate follow-up (not the next roadmap gate; exp56 is reserved for the Ray-free two-node
HPX TCP parcelport probe — likely **exp58**) that reproduces exp50/51's ungraceful-loss timing
(connector dies *while finalize is actively waiting*). The external Ray timeout remains the working
supervisor policy until then.

## Explicit caveats (must read)

1. The reused exp54 clean root uses the instant `dist_probe`, **not** `dist_sleep_probe`.
2. `sleep_ms` affects the **failure** root only, not clean-marker timing.
3. exp55 therefore calibrates an **absolute** clean-marker timeout floor **for this binary**.
4. `clean_root.json` is **pre-finalize**: the detector races **marker arrival**, not distributed
   shutdown/finalize completion.
5. `time_to_finalize_ms` is reported separately and is *not* used to set the detector timeout.
6. Loopback margins are a **lower bound**; a real network/fabric requires separate, likely larger
   calibration. The floor is **not** a portable constant.
7. Phase 0 only observes whether HPX self-bounds a poisoned shutdown on **this** build/config; on this
   path it was inconclusive.
8. This re-validates **no** HPX property. It is detector operating-point calibration, not performance.

## Claim fence

Single-node; loopback TCP; closed-`int64` synthetic action/control only; Ray = supervision/detection
plane only; HPX = execution/data plane inside each island; detector clean-marker timeout calibration
only; the timeout floor is for clean-completion-marker arrival **in this binary**, not a portable HPX
shutdown/finalize guarantee; loopback lower bound only — real network/fabric requires separate
calibration; not HPX fault tolerance; not in-place recovery; no AGAS stale-locality repair; no Ray
actor-failure recovery; no multi-node; no general fabric; no performance/speedup/throughput/latency; no
production/public API; no endpoint seam; no Ray replacement; no "HPX faster than Ray"; no "RayX makes
Ray faster". Future distributed-fabric direction only.

## Reproduce

```bash
# build (exp54 binary is reused unchanged)
cmake --build experiments/54_ray_autonomous_poison_detection/build

# smoke (Phase 1 only, one timeout, 1 repeat)
python experiments/55_poison_detector_operating_point/run_poison_detector_operating_point.py \
    --timeouts 20000 --repeats 1 \
    --skip-preflight --skip-sleep-control --skip-boundary-probe

# preflight smoke (adds the baseline + one shutdown timeout)
python experiments/55_poison_detector_operating_point/run_poison_detector_operating_point.py \
    --timeouts 20000 --repeats 1 --shutdown-timeouts 5000 \
    --skip-sleep-control --skip-boundary-probe

# full run (straddling axis, all phases)
python experiments/55_poison_detector_operating_point/run_poison_detector_operating_point.py
```

Skips cleanly (exit 0) when Ray or the exp54 binary is unavailable.

## Experiment interpretation

- **Passed structurally:** the imported exp54 predicate classifies a clean island `clean_complete` and
  a self-crashed island `poisoned` once `T` clears the marker tail; below the tail the clean arm clips
  to `indeterminate`/`late_clean_result_after_poison` and the detector would false-positive/under-call.
  A stable timeout region exists and an absolute floor + margin is identifiable.
- **What it suggests:** the detector's only real sensitivity is whether `T` exceeds the *healthy*
  marker tail, which on this build is the ~15 s AGAS registration-reflection latency — **not** the
  synthetic `sleep_ms` and **not** the distributed shutdown barrier. exp54's `T=25 s` had healthy
  margin.
- **Hypothesis impact:** confirms the exp54 caveat ("`root_progress_timeout` must exceed healthy
  completion") and pins the dominating term; the original `sleep_ms`-relative model is refuted for this
  binary.
- **Ambiguous / not claimed:** the absolute floor is machine/transport-specific and not portable;
  Phase 0 could not determine whether `hpx.shutdown_timeout` bounds a poisoned shutdown because the
  idle-cap path does not reproduce the exp51 hang.

## Roadmap impact

**Roadmap strengthened** (future distributed-fabric direction): the exp54 detector now has a
characterized operating point and a clear dominating term, with honest scope limits. No in-process
direction change.

### Updated roadmap (directions kept separate)

- **In-process HPX-inside-Ray-actors direction:** unchanged by exp55.
- **Future distributed-fabric direction:** detector operating point characterized (absolute
  clean-marker floor dominated by AGAS registration-reflection latency); the `hpx.shutdown_timeout`
  self-bounding question is **open** because the reused idle-cap path does not reproduce the exp51
  finalize hang. Remains mechanism/calibration evidence only — no performance, fault-tolerance,
  multi-node, production-API, or general-fabric claim.

## Next recommended step

The natural dedicated follow-up is to reproduce the exp51 poisoned-shutdown hang under a focused
shutdown-timeout probe and test whether HPX runtime configuration can bound it. This should be a
separate follow-up, not the next roadmap gate; exp56 is reserved for the Ray-free two-node HPX TCP
parcelport probe. (The shutdown-timeout follow-up is likely **exp58**: reproduce the hang
deterministically — connector dying *while the root is actively in `runtime_distributed::wait()`*, not
seconds before finalize — then test whether `hpx.shutdown_timeout` / `hpx.shutdown_check_count` bounds
it. Only that can resolve whether the Ray timeout is a backstop or the primary policy.)
