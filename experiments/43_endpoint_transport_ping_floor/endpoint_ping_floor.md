# exp43 — local one-shot AF_UNIX endpoint *ping* round-trip floor (runtime-less)

**Status:** closed as an OS/IPC endpoint-transport microprobe. Observation-only.
**Not** HPX-mechanism evidence, **not** a fabric/transport claim, **not** a Runtime
result.

## What this measures

The local round-trip floor of the existing endpoint **`ping`** handshake, with **no
Runtime and no HPX anywhere in the path**, across three same-shape paths reached through
the *real* endpoint API (no spoofed pid, no private lower-level transport helpers):

| Path | What it is | Socket? | Runtime? | HPX? |
|------|------------|---------|----------|------|
| **EP0** | same-process endpoint ping via the registry/local path; `ping` computed inline on the calling thread | no | no | inactive |
| **EP1** | cross-process endpoint ping; child is a transport `Endpoint` **only**; each ping is a fresh one-shot AF_UNIX dial | yes (one-shot/call) | no | inactive both sides |
| **EPraw** | same-shape **Python** control: a plain `socket.AF_UNIX` echo server (no rayx, no framing, no registry); per call connect + 39B request + 16B fixed reply + close. **Not** an OS floor / **not** a lower bound on EP1 | yes (one-shot/call) | no | inactive |

`ping(nonce)` returns the deterministic transform
`nonce ^ ENDPOINT_PING_XOR ^ endpoint_id_hash(peer_id)`. EP0 and EP1 are gated against a
Python oracle (`rayx.endpoint._validate.endpoint_id_hash` + `_rayx._ENDPOINT_PING_XOR`)
over several nonces; EPraw is gated against its fixed 16-byte reply.

## Why this design (HPX-systems-review refinements baked in)

* **EPraw is a same-shape Python control only — it did NOT achieve OS-floor isolation.**
  EPraw's server *and* client are interpreted Python while EP1's serving side is a native
  C++ accept thread, so EPraw is **not** a lower bound on EP1, **not** a minimal C/native
  floor, and does **not** isolate kernel AF_UNIX cost. It shares only the one-shot envelope
  and the 39B/16B byte sizes. Consequently **EP1 remains an undifferentiated end-to-end
  one-shot number** (the HPX-systems-review's stated fallback when no valid OS-floor control
  is present).
* **EP1 is a one-shot dial-per-call round trip.** The remote `Connection` is a probed
  handle, **not** a persistent fd — every ping dials afresh. The dominant term is almost
  certainly socket/connect/accept + fd setup/teardown, **not** moving 55 bytes. It is
  **not** "AF_UNIX transport cost", **not** fabric/persistent-transport cost, **not**
  endpoint→Runtime cost.
* **No `hpx_threads` sweep.** No HPX starts in any path, so there is nothing to sweep.
  Controls record `_process_hpx_active() == False` on parent and child.
* **No mechanical `P2 − EP1` subtraction.** exp42 P2's child runs a live HPX Runtime +
  worker threads contending with its accept thread; exp43 EP1's child has **no** HPX
  workers. EP1 is a runtime-less *lower reference for the total path* (not a guaranteed
  lower bound on any isolated transport component), not a clean subtractor.

## How EP1 relates to exp42 P2 (and where it does not)

* exp42 **P2** = one-shot AF_UNIX envelope + child accept thread + child Runtime bridge
  dispatch + **a live child HPX Runtime** + possible worker-pool contention.
* exp43 **EP1** = one-shot AF_UNIX envelope + accept-thread **inline ping compute**, no
  Runtime, no HPX.

EP1 and exp42 P2 share the *same one-shot transport envelope*. EP1 strips Runtime
dispatch and the second HPX runtime. Because the accept thread in EP1 runs **without** HPX
worker contention, even the transport portion is not apples-to-apples — so `P2 − EP1` is
**not** "Runtime dispatch cost". EP1 only gives an order-of-magnitude runtime-less local
one-shot endpoint ping floor.

## Result (observation-only, this machine)

Full run on **`macOS-26.5-arm64-arm-64bit`** (10 logical CPUs; see `aggregate.json` →
`machine.platform`), reps=200, warmup=30, sequential single-in-flight, GC disabled in the
measured loop:

| Path | median (ns) | IQR (ns) |
|------|------------:|---------:|
| EP0 (same-process inline) | ~208 | ~41 |
| EP1 (cross-process one-shot dial-per-call) | ~9 958 | ~499 |
| EPraw (same-shape Python AF_UNIX echo control) | ~18 709 | ~4 917 |

(Exact integers are in `aggregate.json`; they are **observation-only** and must not be
quoted as a performance result.)

Order-of-magnitude reading only:

* **EP1 − EP0** ≈ +1e4 ns, above run-to-run jitter → `end_to_end_observation_only`. This
  is the cross-process one-shot endpoint round-trip path difference vs the same-process
  inline floor. It is dominated by per-call socket connect/accept/teardown and
  cross-process wake-up, **not** by HPX (there is none) and **not** by payload size.
* **EP1 − EPraw** came out **negative** here, and the sign carries no rayx meaning.
  `interpretation_status = cross_implementation_observation_only`. EPraw's server **and**
  client are **interpreted Python**, while EP1's serving side is a **native C++ accept
  thread**; the Python accept-poll/recv/send loop is simply slower than the native one. So
  EPraw is **not** a lower bound on EP1, **not** a minimal C/native floor, and does **not**
  isolate kernel AF_UNIX cost. EP1 − EPraw is a same-shape (one-shot AF_UNIX, 39B/16B)
  *cross-implementation* observation only, dominated by Python-vs-native server/client
  differences and the EPraw accept-poll loop — it is **not** rayx endpoint overhead, **not**
  listener/framing/registry overhead, **not** an above-the-OS-floor reading, and **not**
  fabric evidence. The two paths share only the one-shot dial-per-call envelope and the
  byte sizes.

## Caveats

* **OS scheduler wake-up.** Sequential single-in-flight pings maximize idle gaps; each
  EP1/EPraw call may pay OS wake-up latency on the blocked `accept()`/`recv()`. This is
  analogous to exp42's HPX idle-backoff caveat, but located in the kernel/OS scheduler.
  EP1 is **not** a stable layer constant; deltas are classified against pooled IQR, not
  the timer floor alone.
* **Non-transferable OS.** AF_UNIX connect/accept cost profiles differ by OS
  (Darwin/macOS vs Linux). These are OS-local observations; see `machine.platform`. If
  this was run on a laptop, the numbers are laptop/Darwin-local IPC observations and are
  non-transferable.
* **EPraw is a same-shape Python control, not an OS floor.** As above, its Python server/
  client interpreter overhead (plus the accept-poll loop) can exceed the native EP1 accept
  path, so it is **not** a lower bound and does **not** isolate kernel AF_UNIX cost. A true
  OS-floor decomposition would need a native C/C++ raw AF_UNIX echo control (experiment-only,
  apples-to-apples with the native dial path) — **not done here**, and out of scope unless
  ever needed.
* **Distributed-fabric fence.** exp43 does **not** inform the distributed-fabric /
  transport question directly. One-shot local AF_UNIX is not a persistent inter-node
  parcelport.

## Allowed claim

> exp43 characterizes the local one-shot endpoint ping round-trip floor without Runtime
> dispatch, using the real endpoint API path (EP0 same-process inline; EP1 cross-process
> one-shot dial-per-call). EPraw is included only as a same-shape Python AF_UNIX echo
> control, not as a raw OS lower bound.

## Required non-claims

No endpoint→Runtime bridge claim · no HPX mechanism / scheduling / value / design result ·
no speedup · no throughput · no general latency claim · no Ray comparison · no distributed
fabric · no persistent transport/channel claim · no parcelport · no AGAS · no multi-node ·
no public endpoint-call API beyond the existing `ping` · no exact decomposition of exp42
P2 · no mechanical `P2 − EP1` subtraction · **no EP1 − EPraw overhead claim** · **no raw
OS floor claim** (EPraw is a same-shape Python control, not an OS floor).

## Structural gates (all PASS)

* EP0 ping correctness vs oracle (nonce sweep + timed nonce).
* EP1 ping correctness vs oracle (nonce sweep + timed nonce).
* EPraw fixed 16-byte reply correctness.
* `parent_hpx_active == False`, `child_hpx_active == False`, `runtime_created == False`
  (and the EP1 child stays HPX-inactive across serving).
* Child starts and exits cleanly; no orphan child (spawn context, piped close + join).
* `aggregate.overall_structural_pass == true`. Timing is observation-only.

## Files

* `run_endpoint_ping_floor.py` — the probe (EP0 / EP1 / EPraw, oracle gates, controls).
* `endpoint_ping_floor.md` — this write-up.
* `aggregate.json` — generated; observation-only timing + structural gates.

## Run

```
python -m py_compile experiments/43_endpoint_transport_ping_floor/run_endpoint_ping_floor.py
PYTHONPATH=python/src python experiments/43_endpoint_transport_ping_floor/run_endpoint_ping_floor.py --smoke
PYTHONPATH=python/src python experiments/43_endpoint_transport_ping_floor/run_endpoint_ping_floor.py
```

## Interpretation and roadmap impact

**Experiment interpretation.** Structurally everything passed: both endpoint paths match
the ping oracle, the Python control matches its fixed reply, and the HPX-inactive /
no-Runtime controls held on parent and child. The **main useful exp43 result is EP1**: an
*undifferentiated end-to-end* one-shot endpoint ping floor — runtime-less and HPX-free —
roughly an order of magnitude above the same-process inline EP0 floor, dominated by per-call
connection setup/teardown and OS wake-up, **not** HPX and **not** payload size. **EPraw is
only a same-shape Python control; it failed to provide an OS-floor decomposition**, because
its Python server/client (plus accept-poll loop) is not comparable to EP1's native C++ accept
thread — so the EP1 < EPraw sign carries no rayx meaning and supports no overhead claim. A
true OS-floor decomposition would require a native C/C++ raw AF_UNIX echo control
(experiment-only, apples-to-apples with the native dial path); **we are not doing that now.**
exp43 **weakens nothing and strengthens nothing** about HPX value; it characterizes the
OS/IPC envelope that sits *underneath* the bridge measured in exp42, and confirms EP1 cannot
be used as a clean subtractor for exp42 P2.

**Roadmap impact: `No roadmap change`.** exp43 is a runtime-less OS/IPC floor probe; it
adds a reference point but does not move either story. In particular it does **not** answer
whether Ray's relevant cost is boundary/orchestration versus actual transport.

**Updated roadmap.**

* *In-process HPX inside Ray actors:* unchanged and still paused for performance work (per
  exp40). The native-composition / boundary characterization arc
  (exp39 → exp40 → endpoint→Runtime bridge v1/v2 → exp42 → exp43) now has its runtime-less
  IPC floor recorded. No new obligation here.
* *Distributed-fabric direction:* still gated. exp43 explicitly does **not** inform the
  distributed-fabric question — one-shot local AF_UNIX is not a persistent inter-node
  parcelport, and remains fenced off from any fabric claim.

**Next recommended step.** If the question is "is the cross-process bridge dominated by
one-shot connection setup rather than the round-trip itself?", add a **persistent-fd**
control (a long-lived connected AF_UNIX pair reused across pings) *as an experiment-only
microprobe* and compare it to EP1's dial-per-call shape — this would isolate connection
setup/teardown from the steady-state round trip, still with no Runtime, no HPX, and no
fabric claim. Do not promote any exp43 path into production endpoint API.
