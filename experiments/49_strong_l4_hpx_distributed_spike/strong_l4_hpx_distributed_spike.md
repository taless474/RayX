# exp49 — Ray-free strong-L4 HPX distributed spike (two phases)

**Structural mechanism-feasibility witness only. Not a performance result. Not a Ray demo.**
This experiment is a standalone HPX binary (`dist_probe_spike`, built from this directory's
`CMakeLists.txt`) and links **no** RayX/production code. It exists to answer, *before Ray
enters the picture*, whether HPX's distributed runtime can carry one registered action
between two processes on one node over loopback TCP.

## Question

Can two plain processes on a single node form **one HPX distributed runtime** over loopback
TCP and execute one registered `HPX_PLAIN_ACTION` from locality 0 to locality 1, returning a
closed `int64` whose value structurally proves it ran on the remote locality — with clean
shutdown and no Ray?

This is the **strong-L4** form (HPX's own AGAS + parcelport carry the call), the genuinely
new path relative to the HPX-free `rayx.endpoint` seam.

## Two-phase design (failure attribution)

The spike is split so a failure is cleanly attributable, per the HPX-expert review that
warned against leading with connect mode:

* **Phase 1 — coordinated launch** (`--role p1_root` / `p1_worker`, fixed
  `--hpx:localities=2`): both processes started together; the console (locality 0) invokes
  the action on locality 1. Answers: *does HPX distributed + AGAS + TCP parcelport + a
  registered action work at all here?*
* **Phase 2 — connect-mode late join** (`--role p2_root` / `p2_connect`,
  `runtime_mode::connect`): a console starts first, a second process joins late, the console
  invokes the same action on it. Answers: *does the Ray-motivated late-join/connect path
  work?*

One binary serves both phases and all roles, so the `HPX_PLAIN_ACTION` is registered
identically in every locality. The action:

```cpp
std::int64_t dist_probe(std::int64_t x) {
    std::uint32_t loc = hpx::get_locality_id();        // the locality that RAN the body
    return (x ^ 0x52415958LL) + (static_cast<std::int64_t>(loc) << 1);
}
```

**Remote proof / oracle.** The console dispatches to the remote locality (id 1) and expects
`(x ^ 0x52415958) + (1 << 1)`. Had the body run locally on the console it would carry
`loc == 0` and the oracle would mismatch — so a match is structural proof of remote
execution. (`int64` in/out only; no payloads, callbacks, or object store.)

## Result (this machine, AppleClang 17, HPX 1.11 networking build)

`overall: pass` — the connect-mode strong-L4 mechanism (Phase 2) **and** its independent-
disconnect lifecycle (Phase 2b) both hold. Phase 1 (coordinated launch) did not rendezvous and
is informational.

| | Phase 1 — coordinated | Phase 2 — connect (passive) | Phase 2b — connect (active lifecycle) |
|---|---|---|---|
| Locality(ies) formed | **no** (`reached_two=false`) | **yes** | **yes** (×2 sequential) |
| Action ran remotely | — | **yes** (`executed_on_locality=1`) | **yes** (both connectors) |
| Oracle match / `proved_remote` | false | **true** | **true** (both) |
| Connector self-`disconnect()` | — | n/a (passive) | **yes, clean** (`post(disconnect)+stop`) |
| Root survives + re-admits | — | n/a (collective teardown) | **yes** (served connector #2) |
| Shutdown | both timed out / SIGKILLed | console clean; joiner exited cleanly | root + both connectors clean, no SIGKILL |

### First-class findings

* **Connect-mode late join works and is the Ray-relevant path.** A separately-started process
  joined the running AGAS over loopback TCP, served the action, and the closed `int64` came
  back proving remote execution. This is the strong-L4 mechanism, end to end.
* **Coordinated bare-process launch did NOT rendezvous.** Across four argument configurations
  (`--hpx:node`+`--hpx:agas` → rejected as incompatible; `--hpx:agas`+`--hpx:hpx` with
  auto-detect → `hpx::init` blocked; `--hpx:nodes` list → address-resolve error; explicit
  `runtime_mode::console`/`worker` → still blocked), `hpx::init` blocked waiting for the
  second locality and `hpx_main` never ran. **Bare `Popen` coordinated launch did not
  rendezvous in this experiment; the root cause is not fully diagnosed.** An mpirun / srun /
  nodefile-style launch may be the relevant path, and an AGAS/parcelport debug-log pass would
  likely pin the cause — but **this is not evidence that HPX coordinated TCP launch is
  unsupported**; connect mode (Phase 2) shows the distributed machinery itself works here.
* **`hpx_main` runs only on the console**, in **both** coordinated and connect modes. The
  second locality (worker or late joiner) is a **passive served locality**: its `hpx_main`
  body does not run, and it is torn down collectively by the console's `hpx::finalize()`. The
  `p1_worker`/`p2_connect` role bodies are therefore effectively defensive; the joiner still
  shut down cleanly (exited on its own, not SIGKILLed).
* **Connect mode needs `--hpx:expect-connecting-localities`** on the root (it defaults to
  `false` at one locality), but does **not** need a fixed `--hpx:localities` count.
* **Working connect-mode args (this build):**
  root → `--hpx:agas=127.0.0.1:P0 --hpx:hpx=127.0.0.1:P0 --hpx:expect-connecting-localities`;
  joiner → `runtime_mode::connect` + `--hpx:agas=127.0.0.1:P0 --hpx:hpx=127.0.0.1:P1`.
  `--hpx:bind=none` warns and is ignored on macOS (harmless).

### Phase 2b — active independent disconnect (passive Phase 2 vs active Phase 2b)

Phase 2's joiner is **passive**: it has no `hpx_main`, serves the action, and is torn down
*collectively* by the root's `finalize()`. That proves independent **join**, not independent
**leave** — and leave is the lifecycle that the future distributed-fabric direction (Ray actor
churn) actually needs. Phase 2b makes the connector **active**: it runs via the non-blocking
`hpx::start` path and drives its own lifecycle (join → wait to be served → self-`disconnect()`).

**Why a second connector is the required evidence.** A single join→serve→leave can pass while
leaving the runtime subtly wedged. The load-bearing proof that the first disconnect was *clean*
(not merely survived) is the root **admitting and serving a second connector afterward**. Phase
2b's pass condition therefore requires both connectors served + self-disconnected and
`root_served_second_connector == true`.

**Phase 2b findings (all `true`, stable across repeated runs):** both connectors joined
(locality ids 1 then 2), both proved remote execution, both called app-level
`hpx::disconnect()` and exited cleanly, the root observed connector #1 leave, **served
connector #2**, and finalized `rc 0` with no SIGKILL.

**Empirical teardown sequence — `post(disconnect)+stop` (the load-bearing detail).** Getting a
clean self-disconnect of a `hpx::start`-launched connect locality took three tries, recorded as
findings: (1) `hpx::disconnect()` from the connector's **main thread** throws
*"this function can be called from an HPX thread only"*; (2) calling it via
`run_as_hpx_thread` makes the **root** clean (it observes the drop, no broken pipe) but the
**connector hangs** — the call never returns once it tears down its own runtime; (3) the
working pattern mirrors production finalize+stop: **`hpx::post([]{ hpx::disconnect(); });
hpx::stop();`** — post the disconnect onto an HPX thread, then join from the main thread via
`stop()`. With the earlier (broken) variants the root hit a parcelport **`Broken pipe` in
`default_write_handler`** for `dist_probe_action`; the `post(disconnect)+stop` pattern removes
it.

## Interpretation

* **What passed structurally:** Phase 2 — two independent processes formed one HPX
  distributed runtime (shared AGAS, TCP parcelport) and a registered action executed on the
  remote locality with a verified closed-`int64` result and clean shutdown. Phase 2b — two
  **sequential** connect-mode connectors each ran the action remotely and **self-disconnected
  cleanly**, with the root **surviving the first disconnect and serving the second**.
* **What the result suggests:** strong-L4 (HPX-native cross-process action) is **mechanically
  feasible on one node**, including the **independent join-and-leave lifecycle** — via
  connect mode, the path that fits independently-launched processes. That is exactly the shape
  Ray imposes. The required teardown for a `hpx::start` connect locality is
  **`hpx::post(disconnect)` then `hpx::stop()`** (not a bare `disconnect()`).
* **What it does NOT establish:** nothing about Ray actors, bootstrap, multi-node, transport
  cost, fault tolerance, or performance. A graceful, self-initiated `disconnect()` is **not**
  fault tolerance — it says nothing about ungraceful locality loss, crash/restart, or AGAS-root
  failure (the AGAS-root/locality-0 SPOF remains open). The coordinated-launch failure is a
  launch-method finding, not evidence that HPX distributed is broken (connect mode proves it
  is not).

## Claim fence

Strong-L4 connect-mode **mechanism + lifecycle feasibility only**; Ray-free; single-node;
**loopback TCP only**; no Ray actor claim; no Ray bootstrap claim yet; no performance / speedup
/ throughput / latency; no multi-node; no general fabric; no fault tolerance; **graceful
disconnect only — graceful disconnect is not fault tolerance**; no crash/restart recovery
claim; no AGAS-root-loss recovery claim; no production / public API; no Ray replacement; no
"HPX faster than Ray". The result is a pass/fail structural witness in the exp41 / exp44
tradition, not a timing.

## Non-claims

* Coordinated launch failing here is **not** a claim that HPX coordinated mode is broken — it
  is only that **bare `Popen` coordinated launch did not rendezvous in this experiment; the
  root cause is not fully diagnosed**.
* Connect mode passing is **not** a claim that it is production-ready, fault-tolerant, or
  multi-node — only that the single-node loopback join/serve/leave lifecycle works (repeatably).
* Phase 2b's clean self-`disconnect()` is a **graceful** leave; it is **not** a claim about
  crash/restart, ungraceful locality loss, or AGAS-root-loss recovery.
* The action proves **remote execution**, not HPX scheduling value, overlap, or parallelism.

## Roadmap

* **Experiment interpretation:** Phase 2 establishes strong-L4 mechanism feasibility via
  connect mode; **Phase 2b establishes the independent join-and-leave lifecycle** (two
  sequential connectors, each self-disconnecting, root surviving and re-admitting). Phase 1 is
  an informative negative (bare `Popen` coordinated launch did not rendezvous; root cause not
  fully diagnosed). Together they ground the *connect-mode* path as the foundation for the
  **future distributed-fabric direction**, because Ray launches actors independently — the same
  constraint under which connect mode, not coordinated launch, is the natural fit.
* **Roadmap impact: Roadmap strengthened (Ray-relevant connect-mode strong-L4 mechanism +
  graceful lifecycle de-risked, still gated).** Two unknowns are answered yes: "does HPX
  distributed work across our processes?" (connect mode + `expect-connecting-localities`) and
  "can a joiner leave gracefully without breaking the root?" (`post(disconnect)+stop`; root
  re-admits a second joiner). This does **not** ungate the future distributed-fabric direction:
  ungraceful fault model, AGAS-root SPOF, oversubscription, and `hpx::start`-inside-a-Ray-worker
  all remain untouched, and nothing here involves Ray.
* **Updated roadmap (tracks kept separate):**
  * *In-process HPX inside Ray actors:* unchanged by this spike.
  * *Future distributed-fabric direction:* the connect-mode join + remote-action +
    **graceful independent disconnect + re-admit** lifecycle is now demonstrated Ray-free on
    one node. Next gated sub-steps remain: hosting the connect-mode locality **inside a Ray
    actor worker** (the `hpx::start`-in-extension question, deliberately excluded here),
    **ungraceful** locality loss / AGAS-root-SPOF / actor-restart behavior (Phase 2b only
    covers *graceful* leave), and parcelport-port vs Ray-networking / oversubscription
    characterization.
* **Next recommended step:** characterize **ungraceful** connector loss (kill a joiner mid-flight
  instead of `disconnect()`) and the AGAS-root/locality-0 SPOF behavior — still Ray-free, still
  standalone — since Phase 2b only covers a graceful, self-initiated leave. Do **not** pull Ray
  forward until the ungraceful-fault behavior is understood.
