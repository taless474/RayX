# exp52 — Ray-orchestrated HPX bootstrap, clean-path island

**Status:** pass (single-node, loopback TCP, clean path only).
**Predecessors:** exp49 proved the Ray-free HPX connect-mode clean lifecycle; exp50/51 characterized
ungraceful loss and established that the HPX island is the failure unit (whole-island external
restart is the recovery boundary). exp52 is the first step of the supervision-plane direction: have
**Ray** launch and bootstrap the already-validated mechanism — clean path only, no failure handling.

## What this is (and is not)

From HPX's point of view **this is the same connect-mode mechanism already validated in exp49**. HPX
cannot observe whether its parent process is a shell, a Python runner, or a Ray actor. Ray is only
the launcher / bootstrap / supervision layer here — analogous to a process launcher such as
`mpirun`, `srun`, or `hpxrun.py`. The HPX action still travels **HPX → HPX over the parcelport**;
Ray carries only bootstrap metadata.

Concretely:

- The **only HPX bootstrap datum carried by Ray is the AGAS endpoint** (`127.0.0.1:p0`), relayed
  driver → actor A → actor B through Ray method calls.
- The **rendezvous files** (`root.ready`, `served1.ok`, `connect.joined1`, `connect.disconnected1`,
  `root_result.json`) are **harness synchronization, not HPX bootstrap and not a data path**. They
  exist only because this is a test; a real integration would not need them.
- The HPX action result (a closed `int64`) **never traverses Ray** — it is computed on the connector
  locality and consumed by the root locality over the parcelport.
- This **re-validates no new HPX property.** It validates the **Ray / process-launch plumbing** for
  the already-proven mechanism.

## Core question

Can Ray act as the process launcher / bootstrap / supervision plane for the HPX connect-mode
clean-path mechanism on one node?

## Design

A local `ray.init()` driver, one role-parametrized `@ray.remote class IslandProcess` with two
instances:

- **Actor A (role=root):** `start_root(meta)` → `Popen` the HPX binary as `f_root` (console, AGAS
  root) with the metadata's ports + rendezvous dir; poll `root.ready`; return metadata only
  (`ready`, `agas_endpoint`, `pid`, launched argv). `wait_exit` / `read_result` / `shutdown` are
  bounded.
- **Actor B (role=connector):** `start_connector(meta)` → `Popen` the HPX binary as
  `f_connect --connector-kind clean` with **A's AGAS endpoint received through Ray** + its own HPX
  port; poll `connect.joined1`; return metadata only (`joined`, `locality_id`, `pid`).
- **Driver:** picks free loopback ports `p0` (root AGAS+HPX) / `p1` (connector HPX) and a `mkdtemp`
  rendezvous dir; starts A first and waits for `root.ready`; relays A's AGAS endpoint into B's
  `start`; lets the HPX action + graceful teardown run; collects results; always tears down both
  actors. All Ray calls and child waits are bounded; stragglers are SIGKILLed by process group.

One fixed registered action, closed `int64 → int64`:
`dist_probe(x) = (x ^ 0x52415958) + (locality_id << 1)` — the executing locality id is folded in as
remote-proof. The root computes the oracle with the connector's locality id and sets
`action_proved_remote = (result == oracle) && (connector_loc != root_loc)`. No managed
`hpx::id_type` is ever returned (closed-value discipline → clean teardown).

Graceful teardown reuses the exp49 empirical path exactly: connector waits for `served1.ok`, then
`hpx::post([]{ hpx::disconnect(); }); hpx::stop();`; the root `wait_id_absent(connector_loc)` (the
**load-bearing graceful-leave gate**, so the root never finalizes before the disconnect propagates —
the exp50 hang mode) then `hpx::finalize()`.

### Folded-in HPX launch-hygiene corrections

1. **Self-locating RPATH, not `DYLD_LIBRARY_PATH`.** A Ray worker may not propagate `DYLD_*` to its
   child (macOS SIP strips `DYLD_*` across many exec boundaries). The binary is built with a baked
   RPATH to the HPX lib dir (`BUILD_RPATH`/`INSTALL_RPATH` + `INSTALL_RPATH_USE_LINK_PATH ON`), and
   the driver launches the child with the **loader env scrubbed** (`DYLD_LIBRARY_PATH`,
   `DYLD_FALLBACK_LIBRARY_PATH`, `LD_LIBRARY_PATH` removed) — so a successful start *proves*
   self-location. Recorded as `binary_self_locating_rpath` / `child_started_without_dyld_env`.
2. **`--hpx:ignore-batch-env` on both** root and connector, so HPX does not auto-detect a batch
   environment (SLURM/PBS) from inherited env vars and override the explicit AGAS/HPX ports.
   Recorded as `hpx_ignore_batch_env_used`.
3. **`--hpx:bind=none` on both** (Ray workers + two HPX localities on one node must not fight over
   core pinning). Numeric `127.0.0.1`, never `localhost`. **No fixed `--hpx:localities=N`** — connect
   mode is dynamic.
4. **Bind/launch-failure classification.** Port TOCTOU is possible; a taken port makes HPX exit
   early rather than hang. The harness distinguishes `root_launch_failed` / `root_bind_failed`
   (child exited before signalling) from `root_ready_timeout` (child still alive but slow), so a
   missing `root.ready` is never misread as a hang.

## Launch arguments (verified present on both children)

- **Root:** `--role f_root --bootstrap <dir> --x 7 --wait-bound 15 --step-timeout 20`
  `--hpx:agas=127.0.0.1:<p0> --hpx:hpx=127.0.0.1:<p0> --hpx:expect-connecting-localities`
  `--hpx:threads=2 --hpx:bind=none --hpx:ignore-batch-env`
- **Connector:** `--role f_connect --connector-kind clean --connector-index 1 --bootstrap <dir>`
  `--serve-timeout 30 --hpx:agas=127.0.0.1:<p0> --hpx:hpx=127.0.0.1:<p1> --hpx:threads=1`
  `--hpx:bind=none --hpx:ignore-batch-env`

## Result (this machine: AppleClang 17, HPX 1.11 networking build; Ray 2.55.1, local; loopback TCP)

| check | value |
|---|---|
| ray launched root / connector actor | yes / yes |
| root child started / connector child started | yes / yes |
| root ready / connector joined | yes / yes |
| root locality / connector locality | 0 / 1 (genuine cross-locality) |
| action proved remote | **yes** |
| action result returned through Ray | **no** (data-plane separation) |
| graceful teardown clean | **yes** (`post(disconnect)+stop`, connector exit rc 0) |
| root finalized clean | **yes** (root exit rc 0) |
| binary self-locating RPATH (scrubbed loader env) | **yes** |
| `--hpx:ignore-batch-env` on both | **yes** |
| bind/launch failure | none |

`overall = pass`. **Stable across 3 consecutive runs** — identical pass on every gate. The binary
also self-locates `libhpx` directly from a shell with a scrubbed loader env (`--hpx:help`, exit 0,
no dyld errors), and `otool -L` confirms `@rpath/libhpx*.dylib` linkage with the baked
`LC_RPATH → /Users/unick/Desktop/Repos/hpx-install/lib`.

## Interpretation

What this **supports:** on one node, a Ray actor can launch an HPX root, a second Ray actor can
launch an HPX connect-mode locality bootstrapped **only** with the AGAS endpoint relayed through
Ray, the HPX root can run a registered closed-`int64` action on that locality (proved cross-locality
over the parcelport, never through Ray), and the island can tear down via the exp49 graceful path
with a clean root finalize. The two launch-hygiene risks specific to a Ray-launched HPX child —
loader-path propagation and batch-env auto-detection — are removed structurally (RPATH self-location,
`--hpx:ignore-batch-env`).

What remains **out of scope / not shown:** this is the **clean path only**. No locality is killed;
the whole-island-fatal policy from exp51 is the **design assumption** under which this is built, not
something exercised here. Ray supervises *processes*; it does not yet handle HPX failure, restart, or
re-bootstrap.

What must **not** be claimed: this is **not fault tolerance**, **not** a new HPX result (HPX cannot
tell its parent is Ray), **not** multi-node, **not** general fabric, and carries **no**
performance/latency claim. Ray is the bootstrap/supervision plane only; HPX remains the
execution/data plane.

## Roadmap impact

**Classification: Roadmap strengthened (clean-path bootstrap leg).** The future distributed-fabric
direction now has its first Ray-orchestrated bootstrap of the validated mechanism, with the
Ray-vs-shell launch differences (loader path, batch-env) characterized and mitigated.

- **In-process HPX-inside-Ray-actors track:** unaffected — exp52 is a distributed-island bootstrap
  property, not an in-process one.
- **Future distributed-fabric direction:** strengthened but still gated. Ray can bootstrap and
  supervise a clean HPX island on one node. The next credible steps are *failure-aware* supervision
  (apply the exp51 whole-island-fatal policy: detect ungraceful loss, kill + restart the whole
  island) and only later a fair multi-node step. This does not pull a fabric/performance claim
  forward; it only establishes the supervision-plane plumbing.

## Next recommended step

One Ray-supervised **failure-and-restart** experiment under the exp51 whole-island-fatal policy
(still single-node, Ray-orchestrated, no performance claim): reproduce the clean bootstrap, then
ungracefully kill the connector actor's HPX child; have the Ray supervisor **detect the poisoned
island and restart the entire island** (root included) rather than attempt in-place repair, and
record whether a fresh Ray-launched island serves a fresh action cleanly. That exercises the policy
exp51 established and is the first thing that makes Ray's supervision role load-bearing rather than
just a launcher.

## Claim fence

Single-node · loopback TCP · closed-`int64` action only · Ray = bootstrap/supervision plane only ·
HPX = execution/data plane inside one island · clean path only · whole-island-fatal policy is
**assumed, not exercised** · no failure injection · no endpoint seam · no production/public API · no
performance/speedup/throughput/latency · no multi-node · no general fabric · **no fault tolerance** ·
no Ray replacement · no "HPX faster than Ray" · no "RayX makes Ray faster" · future
distributed-fabric direction remains gated.

## Reproduce

```
cmake -S experiments/52_ray_bootstrap_clean_island \
      -B experiments/52_ray_bootstrap_clean_island/build \
      -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
cmake --build experiments/52_ray_bootstrap_clean_island/build
python experiments/52_ray_bootstrap_clean_island/run_ray_bootstrap_clean_island.py
```

`build/` is gitignored. The curated `aggregate.json` is tracked; raw per-run logs/bootdirs stay
under per-run temp dirs and are not tracked. **Not part of normal CI** — this is a Ray-capable smoke
tier item that skips cleanly when Ray or the built binary is unavailable (no HPX source build / no
Ray-hosting drivers in normal CI).
