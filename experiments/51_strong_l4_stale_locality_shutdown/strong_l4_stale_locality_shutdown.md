# exp51 — Ray-free strong-L4 stale-locality shutdown / cleanup / recovery-boundary characterization

**Status:** characterized (Ray-free, single-node, loopback TCP only).
**Predecessor:** exp50 showed that an *ungraceful* non-root connect-mode locality loss (SIGKILL)
leaves AGAS/locality state **stale**, still lets the root admit and serve a fresh connector by
**set-difference** targeting, yet makes the root **hang at collective shutdown/finalize** — the
root did **not** self-terminate, and **no** HPX exception enum was thrown. exp50 left one thing
ambiguous: *where* the root hangs, and whether any bounded-finalize or explicit cleanup step could
let finalize complete with a stale dead locality present. exp51 resolves that.

## Question

After ungraceful non-root connector loss leaves stale AGAS/locality state, is there **any HPX-side
bounded-finalize or local-cache cleanup path** that lets the root shut down cleanly — or is the safe
policy **external whole-island restart**?

This is **not** a search for fault tolerance. The likely design answer is already that **no public
AGAS stale-locality eviction API exists** and the island is the failure unit. The value of exp51 is
to (1) **localize** where the root hangs, (2) confirm whether bounded finalize helps on this build,
(3) confirm whether local-cache cleanup helps, and (4) establish external whole-island restart as
the recovery boundary if cleanup fails.

## API archaeology (HPX 1.11 install at `/Users/unick/Desktop/Repos/hpx-install`)

Read directly from the installed headers:

| Mechanism | Symbol | Header | Status | Bearing on the hang |
|---|---|---|---|---|
| Bounded finalize | `hpx::finalize(double shutdown_timeout_us, double localwait_us, error_code&)` | `hpx/hpx_finalize.hpp` | **Public, documented** | The timeout governs **local** HPX-thread drain (*"will not proceed as long as there is at least one pending/running HPX-thread"*); the function still *"block[s] and wait[s] for all connected localities to exit."* That last clause is the membership wait — **no public per-locality timeout**. |
| Remove resolved locality | `hpx::agas::remove_resolved_locality(gid)` | `hpx/components_base/agas_interface.hpp` | Exported, **internal-ish** | Clears the **local resolver cache**, not authoritative AGAS state. |
| Drop parcelport connections | `parcelhandler::remove_from_connection_cache(gid, endpoints)` | `hpx/parcelset/parcelhandler.hpp` (via `applier::get_applier()`) | Exported, **internal-ish** | Clears the **local connection cache**, not authoritative AGAS state. |
| Authoritative locality free | `locality_namespace::free(gid)` | `hpx/agas_base/locality_namespace.hpp` | **Internal virtual** — not user-facing | The only thing that would actually evict a locality from AGAS; **not a public API**. |

**Conclusion:** there is **no public/supported API** to deregister a stale dead locality from AGAS.
`remove_resolved_locality` and `remove_from_connection_cache` are local-cache operations only.
`hpx::terminate()` exists but is non-graceful (`std::terminate` on all localities) — not a clean
path. The HPX resiliency / task-replay modules are task-level, **not** membership/locality-loss
recovery, and are out of scope here.

## Design

One standalone HPX binary (`stale_shutdown_spike`, isolated CMake, **not** wired into `_rayx`),
reusing the exp50 scaffolding: `f_root`/`f_connect` roles, closed-`int64` `dist_probe` /
`dist_sleep_probe`, mid-flight SIGKILL of the victim on the `action_started` marker, set-difference
re-admit, root `--hpx:threads=2`, fresh root per probe, orchestrator-owned wall bounds. Every action
returns a **closed `int64`**, never a managed `hpx::id_type`, so no decref parcel is owed back to the
corpse — whatever teardown behavior we see is attributable to the loss + the cleanup attempt, not to
reference-counting traffic.

Three probes, each on its own fresh root + ports + temp bootdir:

- **P1 — bounded finalize only.** Reproduce the exp50 loss + re-admit; the root writes
  `reached_finalize`, then calls `hpx::finalize(shutdown_timeout_us=5e6, localwait_us=-1)`. The
  orchestrator owns the real wall bound (`--finalize-bound`, default 20 s): if the root does not exit
  within the bound it is **sampled (backtrace) and then SIGKILLed**.
- **P2 — explicit local-cache cleanup, then finalize.** Before the kill, the root snapshots the
  victim's `gid` + endpoints **while it is still alive** (`agas::resolve_locality`). After loss +
  re-admit, from its HPX thread it attempts `agas::remove_resolved_locality(dead_gid)` and
  `parcelhandler::remove_from_connection_cache(dead_gid, endpoints)`, each in try/catch, then bounded
  finalize as in P1. **Refutation expectation:** the dead locality remains authoritative in the
  locality namespace, so clearing local caches cannot cure the membership wait.
- **P3 — whole-island external-restart policy.** Phase 1 reproduces the poisoned root, which the
  orchestrator then **externally SIGKILLs** (no repair attempt). Phase 2 starts a **fresh** root +
  fresh connector on fresh ports/bootdir, serves one `dist_probe`, the connector gracefully
  disconnects, and the fresh root finalizes cleanly. Recorded as
  `external_restart_yields_clean_island` — **policy, not repair, not fault tolerance.**

### Folded-in HPX-expert corrections

1. **Hung-root backtrace before SIGKILL** (load-bearing). For any probe where the root reaches
   finalize and does not exit within the bound, the orchestrator polls for exit **without killing**,
   then `sample <pid> 1 -file …` (fallback `lldb`) the still-live root, stores the raw backtrace
   under the per-run temp dir (ignored), and records `root_hang_backtrace_captured` /
   `root_hang_backtrace_path` / `root_hang_top_frames` — **then** SIGKILLs.
2. **Optional shutdown/AGAS logging** (`--diag`, default off): adds best-effort
   `hpx.logging.level=5` ini; tolerated as a no-op on builds without HPX logging, and never blocks
   the experiment. The backtrace, not the log, is the load-bearing diagnostic.
3. **P2 framed as a refutation**, not likely recovery; cleanup calls wrapped so a throw/abort/
   self-terminate is a **recorded outcome**.
4. **P3 labeled policy, not repair**: it demonstrates a clean *fresh* island, not repair of the
   poisoned root.
5. **Resiliency out of scope** (stated above).
6. **`hpx::terminate()`** noted as the in-process non-graceful analog of killing the island — it
   bypasses clean collective shutdown, so it is **not** a clean recovery mechanism and is not used.

## Result (this machine: AppleClang 17, HPX 1.11 networking build; loopback TCP)

| | P1 (bounded finalize) | P2 (local-cache cleanup) | P3 (whole-island restart) |
|---|---|---|---|
| reached finalize | yes | yes | n/a (poisoned root killed) |
| re-admit served + proved | yes | yes | n/a |
| dead locality still present | yes | yes | n/a |
| cleanup calls returned (no throw) | n/a | **yes (both returned)** | n/a |
| finalize returned clean | **no — hung** | **no — hung** | n/a |
| backtrace captured | **yes** | **yes** | n/a |
| poisoned root confirmed | n/a | n/a | **yes** |
| fresh island finalized clean | n/a | n/a | **yes** |
| `external_restart_yields_clean_island` | n/a | n/a | **true** |

`overall = characterized` (all three probes produced a definite, classified outcome). **Stable
across 3 consecutive runs** — identical per-probe outcomes (P1 hung+backtrace, P2 hung+backtrace+
cleanup-returned-but-uncured, P3 clean fresh island).

**Where the root hangs (backtrace localization).** The sampled hung root's main thread is parked in:

```
hpx::runtime_distributed::run(...)
  -> hpx::runtime_distributed::wait()
    -> hpx::runtime_distributed::wait_helper(mutex&, condition_variable&, bool&)
      -> std::condition_variable::wait(unique_lock&)
```

Worker (scheduler) and io-service threads are **parked idle** (timed CV waits), and only a single
background `parcelhandler::do_background_work` sample appears. So the root is **not** busy-spinning or
retrying the parcelport — it is **blocked on the runtime's shutdown-completion condition variable
that is never notified**, because the collective stop never completes with the dead locality still
in the membership. This **refines exp50's "AGAS barrier vs parcelport cache" ambiguity** toward a
*never-signaled shutdown-completion wait* rather than a parcelport retry loop.

**Bounded finalize does not help (P1).** Passing a finite `shutdown_timeout` (5 s) to
`hpx::finalize` did not unblock the root — consistent with the header semantics: that timeout drains
**local** HPX-threads, but the wait for connected localities to exit has no public per-locality
timeout. `finalize_returned_clean=false`, `finalize_hung=true`.

**Local-cache cleanup does not help (P2) — refutation confirmed.** Both
`remove_resolved_locality` and `remove_from_connection_cache` were called with a pre-kill gid +
endpoints snapshot and **returned without throwing** (`*_threw=false`), yet finalize **still hung**
(`cleanup_cured_finalize_hang=false`). This matches the predicted causal chain: the dead locality is
still authoritative in the **locality namespace**, so the shutdown path still waits on it regardless
of local cache state; clearing local caches is a no-op for the membership wait.

**Whole-island external restart yields a clean island (P3) — policy, not repair.** After confirming
the poisoned root, the orchestrator killed it and a **fresh** root admitted a fresh connector, served
a remote action (`fresh_connector_proved_remote=true`), the connector disconnected cleanly, and the
fresh root **finalized cleanly** (`fresh_root_finalized_clean=true`). This demonstrates that a
*fresh* island is clean — it does **not** repair the poisoned root and is **not** HPX fault
tolerance.

## Interpretation

What this **supports:** on one node over loopback TCP, after an ungraceful non-root locality loss,
there is **no observed HPX-side path to a clean root finalize** — neither the public bounded-finalize
timeout nor the internal-ish local-cache cleanup calls unblock it. The root is blocked in
`runtime_distributed::wait()` on a shutdown-completion CV that is never notified while the dead
locality remains authoritative. The **safe recovery boundary is external whole-island restart**,
which produces a clean fresh island.

What remains **ambiguous:** whether a *supported* authoritative-eviction path could exist in a
custom/resilience HPX build; the exact internal frame that fails to notify the CV (the sample shows
the *waiter*, not the never-arriving *notifier*); and how much of this is HPX-build/OS specific.

What must **not** be claimed: this is **not fault tolerance** and **not crash recovery** — the
runtime did not recover; it stayed usable for a fresh peer but could not shut down cleanly, and the
"recovery" is an external restart of the whole island, not repair. The P2 cleanup APIs are
local-cache/internal-ish, **not** public AGAS eviction. No AGAS-root-loss claim (the victim is a
non-root locality). No Ray actor/bootstrap claim, no performance/latency, no multi-node, no general
fabric.

## Roadmap impact

**Classification: Roadmap narrowed.** exp50 characterized the hang; exp51 now **localizes** it and
**closes** the open question of whether a cheap HPX-side cleanup rescues the root: on this build,
**no**. The concrete obstacle is now precise: a stale non-root locality blocks the root in
`runtime_distributed::wait()` shutdown-completion, and the only reliable recovery is external
whole-island restart.

- **In-process HPX-inside-Ray-actors track:** unaffected — this is a distributed-runtime teardown
  property, not an in-process one.
- **Future distributed-fabric direction:** narrowed. Any future Ray-orchestrated bootstrap reusing
  the exp49 connect-mode mechanism must treat an ungraceful worker-locality loss as **fatal for
  clean shutdown of that island**: the supervisor must SIGKILL and restart the **entire** HPX island
  rather than try to evict the dead locality or rely on a finalize timeout. Graceful disconnect
  (exp49) remains the only clean leave path; bounded finalize (P1) and local-cache cleanup (P2) are
  now ruled out as rescues on this build.

This stays gated: it does **not** pull Ray forward. It sharpens *what to design for* — island as the
failure/restart unit — before Ray is involved.

## Next recommended step

One Ray-free follow-on to close the remaining ambiguity about the *notifier* side: re-run P1 with
`--diag` on a logging-enabled HPX build (or add a targeted instrumentation print at the
`shutdown_all` acknowledgement gather) to capture which locality acknowledgement the root is still
waiting on at the CV — turning "blocked in `runtime_distributed::wait()`" into "waiting for
locality N's shutdown ack." If that confirms the membership-ack gather as the never-notified source,
the design conclusion (island = restart unit) is fully nailed down and the next move can shift to a
**supervised Ray-orchestrated bootstrap** that owns whole-island lifecycle (kill/restart), still
single-node and claim-fenced.

## Claim fence

Ray-free · single-node · loopback TCP only · stale-locality shutdown / cleanup characterization
only · SIGKILLed connector is a crash analog, not a real Ray actor · **no fault-tolerance claim** ·
no crash-recovery generalization · no AGAS-root-loss recovery (victim is a non-root locality) ·
cleanup APIs used are local-cache/internal-ish and **not** public AGAS eviction · whole-island
restart is **external supervision, not HPX fault tolerance** (a clean fresh island, not repair) · no
Ray actor/bootstrap claim yet · no performance/speedup/throughput/latency · no multi-node · no
general fabric · no production/public API · no Ray replacement · no "HPX faster than Ray" · no "RayX
makes Ray faster".

## Reproduce

```
cmake -S experiments/51_strong_l4_stale_locality_shutdown \
      -B experiments/51_strong_l4_stale_locality_shutdown/build \
      -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
cmake --build experiments/51_strong_l4_stale_locality_shutdown/build
python experiments/51_strong_l4_stale_locality_shutdown/run_strong_l4_stale_shutdown.py
# optional best-effort shutdown logging (tolerated if unsupported):
#   python .../run_strong_l4_stale_shutdown.py --diag
```

`build/` is gitignored. The curated `aggregate.json` is tracked; raw backtraces and per-run logs
stay under per-run temp dirs and are not tracked. Not part of normal CI (no HPX source build / no
experiment matrices in CI).
