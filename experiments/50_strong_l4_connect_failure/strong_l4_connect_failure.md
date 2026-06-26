# exp50 — Ray-free strong-L4 ungraceful connect-loss characterization

**Status:** characterized (Ray-free, single-node, loopback TCP only).
**Predecessor:** exp49 proved the *graceful* connect-mode lifecycle — join → remote HPX action
→ self-`disconnect()` via `post(disconnect)+stop` → root re-admits and serves a second
connector. exp50 asks the next, harder question before Ray is ever involved.

## Question

What happens when a connect-mode HPX locality disappears **ungracefully** — SIGKILLed the way a
Ray actor process might crash — and can the root still admit and serve a fresh connector
afterward?

A SIGKILLed connector is a **crash analog, not a real Ray actor.** This experiment characterizes
HPX's observed behavior; it does not promise anything.

## Design

One standalone HPX binary (`dist_fail_spike`, isolated CMake, **not** wired into `_rayx`), two
role families:

- `f_root` — `hpx::init`/`hpx_main` console (AGAS root, locality 0), launched with
  `--hpx:expect-connecting-localities` and **`--hpx:threads=2`**. Branches on `--case A|B|C`.
- `f_connect` — `hpx::start(nullptr)` connect-mode locality. `--connector-kind victim` joins,
  idles, and **never disconnects** (it expects to be SIGKILLed). `--connector-kind clean`
  reuses exp49's graceful `post(disconnect)+stop` path for the re-admit.

Two fixed registered actions, **closed `int64` → `int64`** (the executing locality id is folded
into the result as remote-proof):

- `dist_probe(x)` — short; case-B success path and every re-admit dispatch.
- `dist_sleep_probe(x, millis)` — writes an `action_started` marker as its **first statement**
  (so the orchestrator can SIGKILL the connector while the body is provably executing on it),
  then chunk-sleeps (capped 60 s), then returns the oracle if it ever completes.

### Cases

- **Case A — mid-flight kill (primary).** Root invokes `dist_sleep_probe`; once `action_started`
  appears, the runner SIGKILLs the victim mid-flight. Root classifies the future as
  `returned | threw | timed_out | root_died | no_remote` via a **bounded `wait_for`** (it never
  calls `.get()` after a timeout, so it cannot block the harness).
- **Case B — kill after success (bracketing).** Root serves one short `dist_probe` successfully,
  then the runner SIGKILLs the victim (ungraceful, instead of a graceful disconnect).
- **Case C — kill before dispatch (bracketing, best-effort).** Runner SIGKILLs the victim
  immediately after it joins, before the root dispatches. This is **timing-fragile** (it races
  whether the TCP/AGAS layer has noticed) and is **non-gating** — it runs and is recorded but
  does not decide `overall`.

In every case the root then attempts a re-admit with a clean connector #2.

### Folded-in HPX-expert corrections

1. **Root self-termination is a first-class outcome.** HPX's fatal-error path can abort the root
   process on a broken parcelport. The runner records `root_self_terminated` / `root_exit_signal`
   **distinctly** from a harness-killed `root_hung`; it never infers a timeout from a missing
   result file. **For Case A, root self-termination is a likely and valid outcome, not a harness
   bug.**
2. **Re-admit by set-difference.** The root snapshots `pre_kill_localities` (including itself and
   connector #1), then targets a locality id **not present before the kill** — robust to AGAS
   retaining the dead locality *or* reusing its id. No hard-coded dead id. If no new locality
   appears it records `new_locality_after_loss_seen=false` and does not hang.
3. **Root runs `--hpx:threads=2`** in all cases (so the bounded wait, the serve, and the
   `find_all_localities()` polling do not contend on one worker).
4. **HPX error enum captured on throws** — `catch (hpx::exception const&)` before
   `std::exception`, recording the `hpx::get_error` enum name + numeric value + `what()`.
5. **Closed-value shutdown rationale** (below).
6. **Case C is explicitly best-effort / non-gating.**
7. **No wall-time-to-detection latency metric** — only the booleans `detected_before_wait_bound`
   / `detected_at_full_bound` / `timed_out_at_bound`.

### Why closed values keep teardown clean to characterize

Every action returns a closed `int64`, **never a managed `hpx::id_type`**. So no global-reference
credit/decref parcel is ever owed back to a (possibly dead) locality at shutdown — if the actions
returned managed ids, their destructors would try to decref to the corpse and could hang on that
alone, confounding the measurement. The only ids in play are the **unmanaged** locality ids from
`find_all_localities()`, which carry no reference credit. The probe is deliberately closed-value so
that whatever teardown behavior we observe is attributable to the locality loss itself, not to
reference-counting traffic.

## Result (this machine: AppleClang 17, HPX 1.11 networking build; loopback TCP)

| | Case A (mid-flight) | Case B (after success) | Case C (before dispatch, best-effort) |
|---|---|---|---|
| connector1 served | n/a (long action) | yes (`returned`) | n/a |
| connector-loss future | `timed_out` at bound | `returned` (served pre-kill) | root hung before dispatch |
| HPX exception | none | none | none |
| AGAS retained dead locality | yes (3 localities) | yes (3 localities) | — |
| set-difference re-admit | **served + proved_remote** | **served + proved_remote** | not reached |
| root finalized clean | no — **hung at shutdown** | no — **hung at shutdown** | no — **hung** |
| root self-terminated | no | no | no |

**What the run shows on this machine:**

- The loss did **not** surface as a future exception. In Case A the root's bounded `wait_for`
  simply **timed out** (`detected_at_full_bound=true`); the parcelport raised nothing. No
  `hpx::error` enum was produced in any case.
- **AGAS retained the dead locality** (stale): `localities_after_loss=3` (root 0, dead 1, new
  connector2 2), `dead_locality_still_present=true`. HPX's default build has no failure detector
  that reaps it.
- **The runtime stayed usable across the loss for a fresh peer:** in A and B the root admitted a
  brand-new connector #2 by set-difference and served it (`connector2_proved_remote=true`). A
  naïve "first non-self locality" re-admit would instead have re-targeted the stale dead
  locality 1 — the set-difference correction is load-bearing here.
- **But the root never finalized cleanly.** In every case the root **hung at collective
  shutdown** with the stale dead locality present and had to be SIGKILLed by the harness
  (`root_hung=true`, `root_finalized_clean=false`, exit signal 9). The expert's predicted
  *self-termination* did **not** occur on this machine — the failure mode here is a **hang**, not
  a crash or a thrown exception. The schema's hung-vs-self-terminated distinction is what lets us
  say that precisely.
- Connector #2's own graceful disconnect was **not** confirmed clean in these runs
  (`connector2_disconnected_clean=false`) — consistent with the same stale-locality collective
  path that wedges the root's finalize also impeding the connector's graceful leave. Recorded as
  an observation, not a mechanism claim.

`overall = characterized` (cases A and B each produced a classified outcome and the harness
completed; Case C is non-gating). **Stable across 3 consecutive runs** — identical per-case
outcomes (A `timed_out`+hung+readmit, B `returned`+hung+readmit, C hung), so the classification
is not a one-off race.

## Interpretation

What this **supports:** ungraceful connector loss, on one node over loopback TCP, leaves the HPX
runtime *usable for serving a fresh connector* (re-admit + remote action succeed) but leaves AGAS
**stale** and makes the root's **collective finalize hang** on the dead locality. The interesting
cost of an ungraceful loss here is at **teardown / collective shutdown**, not at the next
data-plane dispatch.

What remains **ambiguous:** whether the hang is specifically in the AGAS shutdown barrier vs the
parcelport connection cache; whether a longer bound or an explicit "evict locality" step would
let finalize complete; whether connector #2's unclean disconnect is the same root cause; and how
much of this is HPX-build/OS specific.

What must **not** be claimed: this is **not fault tolerance.** The runtime did not *recover* from
the loss — it tolerated a new peer but could not shut down cleanly. No crash-recovery
generalization, no AGAS-root-loss claim (out of scope; the victim is a non-root locality), no Ray
actor/bootstrap claim, no performance/latency, no multi-node, no general fabric.

## Roadmap impact

**Classification: Roadmap narrowed.** The in-process and future-fabric pictures are unchanged in
direction, but a concrete obstacle is now characterized: ungraceful non-root locality loss does
not crash the root or block the next dispatch here, yet it **poisons clean collective shutdown**.

- **In-process HPX-inside-Ray-actors track (Track A):** unaffected; this is a distributed-runtime
  teardown property, not an in-process one.
- **Future distributed-fabric direction:** narrowed. Any Ray-orchestrated bootstrap that reuses
  the exp49 connect-mode mechanism must plan for the fact that a crashed worker locality leaves
  AGAS stale and can hang the root's finalize — i.e. Ray actor lifecycle (kill/restart) will hit
  this exact teardown hang unless an explicit locality-eviction or supervised-shutdown step is
  designed. Graceful disconnect (exp49) remains the only clean leave path demonstrated.

This stays gated: it does **not** pull Ray forward. It tells us *what to design for* before Ray.

## Next recommended step

One Ray-free follow-on that isolates the hang: re-run Case A but, after the bounded
`wait_for` timeout, have the root attempt an **explicit locality-eviction / shutdown-with-timeout**
(e.g. a bounded `hpx::finalize` shutdown timeout, or dropping the stale locality from the shutdown
set if HPX exposes a supported way) and record whether finalize can be made to complete cleanly
with a stale dead locality present. If no supported clean-finalize path exists, document that as
the concrete gate Ray orchestration must work around. Do **not** start Ray bootstrap until the
shutdown-hang is understood.

## Claim fence

Ray-free · single-node · loopback TCP only · ungraceful connect-mode connector-loss
characterization only · SIGKILLed connector is a crash analog, not a real Ray actor · **no
fault-tolerance claim** (even though the root re-admitted a fresh connector, phrase it narrowly:
the runtime stayed usable for a new peer but did **not** shut down cleanly) · no crash-recovery
generalization · no AGAS-root-loss recovery (out of scope) · no Ray actor/bootstrap claim yet · no
performance/speedup/throughput/latency · no multi-node · no general fabric · no production/public
API · no Ray replacement · no "HPX faster than Ray" · no "RayX makes Ray faster".

## Reproduce

```
cmake -S experiments/50_strong_l4_connect_failure \
      -B experiments/50_strong_l4_connect_failure/build \
      -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
cmake --build experiments/50_strong_l4_connect_failure/build
python experiments/50_strong_l4_connect_failure/run_strong_l4_connect_failure.py
```

`build/` is gitignored. The curated `aggregate.json` is tracked; raw logs/bootdirs stay under
per-run temp dirs and are not tracked. Not part of normal CI (no HPX source build / no experiment
matrices in CI).
