# exp63 — A1 connector-lifetime race: diagnosis and minimal hardening

**Status:** mechanism / lifecycle evidence. Not performance evidence. No Ray, no payload, no same-axis
comparison, no collectives, no production runtime. No ratio / speedup / winner language.

This report documents (1) the diagnosis of the exp63 A1 `when_all_then_reduce` "before_dispatch
`std::system_error` / Operation not permitted" fault as a **connector serve-window / lifecycle race**,
and (2) the minimal connector-lifetime hardening that fixes it, proven with a same-allocation A/B.

## The fault as originally seen

The instrumented A1 diagnostic (`--phase a1-diagnostic --composition-mode when_all_then_reduce`)
reported, after a handful of successful calls:

* `exception_stage = before_dispatch`
* `exception_type = std::system_error`
* `exception_code = 1 / generic` (message "Operation not permitted")

The errno-1 message reads like a kernel permission failure. It is not.

## Diagnosis evidence

1. **Syscall trace found zero EPERM.** Running A1 under `strace -f` over the targeted syscall set
   (`clone*`, `sched_set*`, `mbind`/`set_mempolicy`/`migrate_pages`, `socket`/`connect`/`close`/
   `shutdown`, `prlimit64`/`setrlimit`) produced **no `= -1 EPERM`** across the full trace — including a
   large `ECONNREFUSED` reconnect storm to departed remote localities. There is no live kernel
   permission syscall behind the "Operation not permitted".

2. **The real signal is connector-side.** The connector's flushed HPX diagnostic showed, on a
   parcelport thread:

   ```
   HPX(invalid_status): thread pool is not running
     at scheduled_thread_pool::create_work
     during parcel::load_schedule
   ```

   i.e. a leaf-action parcel arrived at a connector whose HPX thread pool had **already stopped**, and
   `create_work` raised an internal `invalid_status`. The `std::system_error` errno-1 surfaced on that
   lifecycle-state error; it is not a kernel EPERM.

3. **Hold-open confirmation (job 159058).** A1 `when_all_then_reduce` with `serve-timeout 600`
   completed cleanly **20/20**, `wait_for_status = ready`, no exception; both connectors stayed alive
   through the run and disconnected gracefully. Holding the serve window open removed the fault.

4. **Serve-timeout sweep (job 159059).** Fixing everything except the connector serve-timeout:

   | serve-timeout (s) | result | fault call index |
   | --- | --- | --- |
   | 90  | fault | 7 |
   | 150 | fault | 14 |
   | 300 | pass 20/20 | — |
   | 600 | pass 20/20 | — |

   The fault index scales with the serve window, then plateaus into a clean pass once the window
   covers the whole run. That monotonic boundary is the signature of a **serve-window race**, not an
   intrinsic composition permission/progress fault.

## Root cause

Connector lifetime was governed by a **fixed wall-clock serve-timeout** measured from join. The
connector left (its HPX runtime stopped) when that timeout expired, even if the root was still
dispatching. A leaf parcel dispatched into the just-stopped connector pool then hit `create_work` on a
stopped pool → `invalid_status` → the root observed a `before_dispatch` `std::system_error` errno-1.

## The minimal fix (connector-lifetime hardening)

`serve-timeout` becomes a **deadman safety guard for root silence**, not the normal lifetime boundary:

* The **root** writes a per-connector heartbeat file (`root.alive`) and touches its mtime **before every
  dispatch** (prewarm and timed).
* The **connector** wait loop resets a deadman timer, measured on the connector's **own steady clock**
  (robust to cross-node wall-clock skew), whenever the `root.alive` mtime advances. The `serve-timeout`
  deadman fires only if the root has been **silent** — no heartbeat — for `serve-timeout` seconds.
* The **root** writes a completion sentinel (`root.done`, plus the legacy `served1.ok`) **only after all
  prewarm + timed calls are done and the artifact is captured** — i.e. no more remote dispatches will
  occur.
* The **connector** exits on the completion sentinel (`connector_shutdown_reason =
  root_completion_signal`); it exits on the deadman only if the root goes silent
  (`serve_timeout_expired`); a teardown error dominates (`error`); otherwise `unknown`.

With per-dispatch heartbeats, a low `serve-timeout` covers an arbitrarily long run as long as the root
keeps dispatching. If the runner is an older build that never heartbeats, the deadman degrades to the
previous fixed-timeout behavior (backward compatible).

## A/B validation (job 159061, same allocation, serve-timeout = 90)

`serve-timeout = 90` is the setting that previously faulted at call 7.

| Run | heartbeat | calls_ok | fault | connector shutdown reasons | stayed-alive | late-parcel inferred |
| --- | --- | --- | --- | --- | --- | --- |
| Hardened | on (default) | **20/20** | none | `root_completion_signal` ×2 | True | False |
| Control (`--no-completion-signal`) | off | 7 | `std::system_error 1/generic` @ before_dispatch | `serve_timeout_expired` ×2 | False | **True** |

The only difference between the pass and the fault is the heartbeat / completion contract. The control
reproduces the exact original fault and self-classifies it as a serve-timeout / late-parcel race; the
hardened run completes and both connectors leave via root completion. Connector provenance
(`connect.disconnected1`) recorded the reasons directly, all with `clean = true`.

## Artifact fields added

Per connector (`connect.disconnected1`, surfaced in the A1 artifact `connectors[]`):
`connector_lifetime_mode`, `connector_shutdown_reason`, `root_completion_signaled`,
`serve_timeout_expired`, `serve_timeout_s`, `connector_stayed_alive_until_root_done`,
`root_completion_signal_time`, `connector_observed_completion_time`.

Top-level A1 artifact: `connector_lifetime_mode`, `heartbeat_enabled`, `serve_timeout_s`,
`connector_shutdown_reasons`, `root_completion_signaled_all`, `serve_timeout_expired_any`,
`connectors_stayed_alive_until_root_done`, `late_parcel_after_shutdown_detected` (an **inference** from
a before_dispatch `system_error` co-occurring with any `serve_timeout_expired` connector — not a direct
connector-side capture).

## Caveats

* Heartbeat cadence must stay **below** `serve-timeout`. With per-dispatch heartbeats,
  `dispatch-timeout 8 s`, and `serve-timeout 90 s` there is wide margin.
* A pathologically slow **single** dispatch (> `serve-timeout` of silence) can still trip the deadman —
  by design; that is the intended safety behavior, not a regression.
* The mtime-change deadman relies on **shared-filesystem mtime visibility** across nodes (Rostam's
  shared FS surfaces mtime updates promptly).
* **JSON filename clobbering:** the a1-diagnostic writes a fixed JSON filename, so successive runs (e.g.
  hardened then control) overwrite it on disk. The tee logs and per-run connector provenance preserved
  both runs' evidence; a future runner should encode timeout / port / mode in the artifact filename.

## What this does and does not license

* This is mechanism / lifecycle evidence only. No performance, Ray, payload, same-axis, or collective
  claim. No ratio, speedup, or winner language.
* `root_flat_gather_poll` remains the proven interim cross-node path for exp62 and early exp64.
* Native `when_all_then_reduce` is cleared of the specific "permission / progress fault" charge **only
  under a correct connector-lifetime contract** (connectors alive while the root dispatches). This does
  **not** claim native composition is robust under all timings, transports, or scales.

## Artifacts

Gitignored under `_exp63_runs/` (copied back from Rostam; source not synced back):

* `a1_strace_copyback_159057/` — strace log + connector diagnostic note (zero-EPERM evidence).
* `a1_holdopen_clean_copyback_159058/` — hold-open pass (serve-timeout 600).
* `a1_serve_timeout_sweep_copyback_159059/` — the 90/150/300/600 sweep (tee logs + provenance).
* `a1_lifetime_fix_copyback_159061/` — the hardened-vs-control A/B (tee logs + provenance).
