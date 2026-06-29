# exp56 — Ray-free two-node HPX TCP parcelport connect-mode probe

> **Status: Part 1 PASSED on Rostam (two physical nodes).** Local Part 0 de-risking is complete, and
> the two-node connect-mode mechanism is demonstrated on real nodes (manual runs on both eno16 and
> ibp94s0; the patched runner reproduced the curated pass on eno16). Within its narrow scope this gate
> is **closed** — but read the claim fence (§5): this is one TCP two-node mechanism probe, **not** a
> distributed RayX, an HPX fabric, a performance, or a fault-tolerance result.

## 1. Purpose

exp56 is the **first future distributed-fabric gate**: a Ray-free probe that isolates **HPX inter-node
TCP parcelport communication** in connect mode. It deliberately keeps Ray out so the one new variable
is the HPX TCP parcelport across two physical nodes — nothing else. The pass condition (Part 1) is:
**root on node A, connector on node B, one closed-int64 HPX action over the real-network TCP
parcelport, three-way remote proof, graceful disconnect, clean finalize, no orphans on either node.**

This document records that **Part 0 / local de-risking is complete and passed locally** (§2–§3), and
that **Part 1 / two-node evidence has now PASSED on Rostam** (§4). The TCP parcelport is load-bearing
for connect-mode (MPI/LCI-style parcelports generally assume a statically launched communicator and
may not support independent `runtime_mode::connect` join/leave); exp56 therefore probes the **dynamic
mechanism over TCP**, not the HPC performance transport path.

## 2. Part 0 — local de-risking (passed locally, the five checks)

All five ran on the local darwin machine against the HPX install at
`/Users/unick/Desktop/Repos/hpx-install`. These are **local de-risking only**; they do not prove
two-node behavior.

1. **Build test — passed.** `two_node_tcp_spike` (derived from the exp49 connect-mode core, self
   contained in exp56, with a per-locality hostname/endpoint self-attestation marker) configures and
   links cleanly via the experiment-local `CMakeLists.txt`.
2. **Skip-path tests — passed, exit 0.** Binary missing → `overall=skip`. No ≥2-node Slurm allocation
   → `overall=skip` with Part 0 results retained and a clear "deferred to Rostam" reason.
3. **Command-line validation — passed.** The binary exposes `--role {root,connect}`, `--bootstrap`,
   and the timeouts; the orchestrator exposes `--phase {check-config,diag-loopback,reachability,run,
   all}`. Two real HPX-flag issues were found and fixed locally (see §3).
4. **Loopback diagnostic — passed.** Full single-node connect-mode lifecycle: root started, connector
   genuinely joined (oracle match **and** `remote_locality_id_differs` both true), graceful disconnect,
   clean finalize. Settle ~100–101 ms, stable across three runs (see §3).
5. **Aggregate shape check — passed.** All required fields present; two-node fields correctly `null`
   under the skip; single-node evidence isolated under `loopback_diag` with an explicit "does NOT prove
   two-node" note.

## 3. Important local findings

**HPX build / parcelport configuration**
- The **local HPX build has the TCP parcelport** (`--hpx:dump-config` shows `[parcel] bootstrap: tcp`
  and `[parcel][tcp] enable: 1`; no MPI/LCI sections on this build).
- **TCP pinning works** with `--hpx:ini=hpx.parcel.bootstrap=tcp` and
  `--hpx:ini=hpx.parcel.tcp.enable=1` (both accepted).
- **Do not blindly disable absent parcelports.** HPX **rejects unknown ini keys**
  (`hpx.parcel.mpi.enable=0` → `HPX(no_success)` on this TCP-only build). The orchestrator disables
  only parcelports that `check-config` actually finds present.
- **`--hpx:dump-config` does not exit early** — it dumps the resolved ini and then still runs the
  root. The config probe therefore passes explicit endpoints + `--ready-timeout 1` so it dumps and
  exits in ~1 s instead of waiting for a connector.

**Loopback settle diagnostic (the Part 0c question)**
- **Settle ~100–101 ms** in the exp56 binary (root readiness → both localities visible), **stable
  across three runs**.
- **Advertised endpoints match intended `127.0.0.1`** for both root and connector.
- **No retry / refused / timeout pattern** in the parcel/HPX logs.
- **No wrong-interface advertisement** found.
- This exp56 loopback result **does not reproduce exp54/55's ~15 s marker tail.**

**Honest interpretation of the settle finding.** No endpoint-advertisement or retry pathology was
found in the clean exp56 connect-mode path: endpoints are correct, there are no retries, and
registration reflection is sub-second. This is reassuring for the two-node pivot — the HPX-expert's
concern that the old ~15 s might be a wrong-interface/retry problem (benign on loopback, fatal across
nodes) is **not present** in this binary's path. However, this is **not a controlled root-cause
analysis of the old 15 s**: exp56 uses a different (minimal) binary and measures the in-binary
`root.ready → two-localities` span, whereas exp55 measured an orchestrator-to-marker span on the exp54
binary. The defensible conclusion is "no advertisement/retry pathology to fix before Part 1," not "the
exp54/55 15 s is explained."

## 4. Part 1 — PASSED on Rostam (two physical nodes)

Part 1 ran on Rostam (Slurm) and **passed**. exp56 is **Ray-free**: the only moving part is the HPX
TCP parcelport across two physical nodes. **Root ran on `medusa00`; the connector ran on `medusa01`.**

**Curated patched-runner pass (eno16, `10.42.5.30` / `10.42.5.31`).** Command sequence:

```bash
python3 run_two_node_tcp.py --phase check-config
python3 run_two_node_tcp.py --phase diag-loopback
python3 run_two_node_tcp.py --phase reachability --prefer-subnet 10.42.5.
python3 run_two_node_tcp.py --phase run --prefer-subnet 10.42.5. --agas-port 7940 --hpx-port 7941
```

- **check-config:** `tcp_parcelport_available=True`, `config=(bootstrap=tcp; tcp=on)`.
- **diag-loopback:** `loopback_settle_ms=100`; advertised endpoints match intended `127.0.0.1` (root
  `127.0.0.1:45761`, connector `127.0.0.1:42689`); `wrong_iface=False`; no retry/refused/timeout in the
  logs.
- **reachability:** socket-only preflight passes both directions — `bidi=True`, `b_to_a=True`,
  `a_to_b=True`, nodes `medusa00`/`medusa01`, IPs `10.42.5.30`/`10.42.5.31`.
- **two-node run:** `overall=pass`, `proved_remote=True`, `agas_settle_ms=30316`, `no_orphans=True`.
  The three-way remote proof holds: closed-int64 oracle match, connector locality id ≠ root locality
  id, and connector hostname/IP ≠ root's (side-channel attestation, never in the action result).

**Manual secondary evidence (both subnets).** Independent manual two-node runs reproduced the remote
proof on **both** the Ethernet (`eno16`) and IPoIB (`ibp94s0`) subnets:

- **eno16:** root `medusa00 10.42.5.30:7920`, connector `medusa01 10.42.5.31:7921`; `ROOT_RC=0`,
  `CONN_RC=0`, `localities_seen=2`, `reached_two=true`, `remote_locality=1`, `invoked=true`,
  `result=1380014433` matching `oracle=1380014433`, `executed_on_locality=1`,
  `remote_locality_id_differs=true`, `proved_remote_by_oracle=true`, observed connector leave, clean
  disconnect, no orphans.
- **ibp94s0:** root `medusa00 10.42.6.30:7930`, connector `medusa01 10.42.6.31:7931`; same remote proof
  passed; `settle_ms≈30213–30216`.

Both manual runs had oracle match, differing remote locality id, differing root/connector hostnames,
clean disconnect, `rc=0`, and no orphans.

**Reading the numbers honestly.**
- `agas_settle_ms≈30 s` is **structural readiness** (root readiness → both localities visible →
  remote action serviceable), **not** latency or performance.
- `no_orphans=True` is established via `pgrep`: an exit code of `1` means *no matching process found*,
  therefore no orphan process on either node.

**Previous failures were runner / environment issues, not HPX mechanism failures.** Before the patched
runner closed the gate, three non-HPX problems had to be fixed:

1. **Node-local `/tmp` bootstrap directory.** The original two-node bootstrap defaulted to `/tmp`,
   which is node-local under Slurm on Rostam: the root on `medusa00` wrote markers to `medusa00`-local
   `/tmp`, and the launcher/connector could not see them, so the runner reported `root_started=false`.
   Fixed by defaulting multi-node runs to an experiment-local `_two_node_runs/` directory under shared
   `/work`.
2. **Synced Mac `build/` polluting Rostam.** A Mac `build/` got rsynced to Rostam; its stale
   `CMakeCache.txt` referenced `/Users/unick/...` and a Homebrew Ninja path, so the wrong binary/cache
   produced misleading failures. Fixed by deleting the Rostam `build/` and rebuilding natively; future
   syncs must exclude `build/` and `*/build/`.
3. **GCC 15 `libstdc++` not propagated through `srun`.** The native binary needs GCC 15 symbols
   (`GLIBCXX_3.4.32`, `GLIBCXX_3.4.30`, `CXXABI_1.3.15`), but the `srun` root step initially loaded the
   system `/lib64/libstdc++.so.6` because the runner had scrubbed the loader environment. Fixed by
   preserving the environment for child/`srun` processes and running under the GCC 15 module
   environment.

Within its narrow scope (§5), the two-node mechanism gate is now **closed**.

## 5. Claim fence

- **Local validation does not prove two-node.** Loopback does not prove a distributed fabric.
- No Ray (Ray is out entirely until the bare two-node HPX mechanism is green and stable).
- No performance / speedup / throughput / latency claim. `agas_settle_ms` and `loopback_settle_ms` are
  **structural readiness** durations, not latency or performance.
- No HPX fault tolerance; no Ray actor-failure recovery.
- No production / public API; no object store; no arbitrary Python; no Ray replacement.
- First two-node probe only; no general fabric claim from one TCP two-node probe.
- Closed-int64 action only.
- **No MPI/LCI performance-path claim.** The **dynamic-supervision / high-performance-parcelport
  tension remains unresolved**: exp56 targets the dynamic mechanism over TCP (off the HPC performance
  transport path), and even when Part 1 passes it will not be a performance-path result.
- "Future distributed-fabric direction," not Track B / track_b.

## 6. Aggregate interpretation

The committed `aggregate.json` is the **two-node Rostam `run`-phase aggregate** and carries
`overall="pass"`:
- **Parcelport / config:** `tcp_parcelport_available=true`, `tcp_parcelport_pinned=true`,
  `other_parcelports_disabled_or_not_selected=true`, `hpx_parcelport_config="bootstrap=tcp; tcp=on"`.
- **Two-node fields populated:** `two_node_run=true`, `nodeA="medusa00"`, `nodeB="medusa01"`,
  `nodeA_ip="10.42.5.30"`, `nodeB_ip="10.42.5.31"`, `selected_interface="eno16/eno16"`,
  `bidirectional_port_check_passed=true`, `root_started=true`, `connector_joined=true`,
  `agas_settle_ms=30316`.
- **Three-way remote proof:** `action_proved_remote_by_oracle=true`, `remote_locality_id_differs=true`,
  `remote_hostname_or_ip_differs=true`, and the rollup `action_proved_remote=true`. Attestation
  hostnames are `medusa00.rostam.cct.lsu.edu` / `medusa01.rostam.cct.lsu.edu`.
- **Teardown / hygiene:** `graceful_disconnect_clean=true`, `root_finalized_clean=true`,
  `no_orphans=true`, `orphan_pids=[]`.
- **Loopback fields `null` in this aggregate.** The committed aggregate is the two-node `run` phase, so
  the single-node `loopback_*` / `loopback_diag` fields are `null`; the loopback diagnostic
  (`loopback_settle_ms=100`, correct `127.0.0.1` endpoints, no retry/wrong-interface) was produced by
  the separate `diag-loopback` phase (§4).
- **`overall="pass"`** here reflects real two-node execution (three-way remote proof + clean teardown +
  no orphans on both nodes), not a skip placeholder.

## 7. Next concrete step

exp56 is closed within its scope. The next step is **exp57: a Ray/Slurm-supervised two-node HPX
island** — Ray/Slurm launches root on node A and connector on node B and only supervises bootstrap and
lifecycle, while HPX keeps the execution/data path. exp57 should reuse this proven two-node TCP
parcelport mechanism and preserve the whole-island restart policy from exp53/54. No production API yet.

## 8. Roadmap impact

**Roadmap strengthened.** Part 0 de-risking removed the concrete launch/config risks (TCP
availability, ini-key rejection, dump-config behavior, endpoint advertisement, no retry pathology), and
Part 1 has now passed on two physical Rostam nodes (curated patched-runner pass on eno16, plus manual
secondary evidence on both eno16 and ibp94s0). The first two-node mechanism gate of the future
distributed-fabric direction is closed within its narrow scope.

- **In-process HPX-inside-Ray-actors direction:** unaffected by exp56.
- **Future distributed-fabric direction:** exp56 supplies the first measured two-node connect-mode
  mechanism evidence (Ray-free, TCP parcelport, closed-int64 action). No performance, fault-tolerance,
  production-API, MPI/LCI performance-path, or general-fabric claim is made or implied; this is one
  mechanism probe, and it does not by itself license a multi-node performance comparison.

**Next recommended step:** as §7 — exp57: bring Ray/Slurm in as supervisor/launcher over this
already-proven two-node HPX island, keeping HPX on the execution/data path.
