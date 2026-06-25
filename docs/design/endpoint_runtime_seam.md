# Endpoint → Runtime seam (consolidated design)

Exploratory design note (not a stable shipped reference doc). This is the **canonical,
consolidated** narrative for the experimental `rayx.endpoint` arc — it supersedes and
folds together four earlier notes:

* `endpoint_identity_seam.md` (identity seam) → §2,
* `endpoint_transport_slice.md` (A1 local transport) → §3,
* `endpoint_runtime_shared_owner.md` (shared HPX owner, Variant 2) → §4,
* `endpoint_runtime_bridge.md` (endpoint→Runtime bridge v1/v2) → §5.

Those four files are retained as short pointer stubs. Their per-run validation numbers,
file changelists, and slice-by-slice changelog provenance are **intentionally not retained**
in this consolidation (they were working-tree-only design notes, not committed history) —
this note keeps only the stable, durable design narrative. The observation-only
boundary/IPC measurements live in `experiments/42_endpoint_bridge_boundary_cost/` and
`experiments/43_endpoint_transport_ping_floor/` and are summarized in §6.

---

## 1. Purpose and scope

`rayx.endpoint` is a **local identity / discovery seam** plus a narrow local delivery
path. A reader can follow it end to end: **endpoint identity → local transport → shared
Runtime ownership → endpoint→Runtime bridge → boundary/IPC interpretation.**

The load-bearing scope discipline:

* The **endpoint layer is intentionally HPX-free.** Constructing an `Endpoint` does not
  start HPX; the ping is a pure inline transform; the transport listener is a plain OS
  accept thread.
* **The `Runtime` owns HPX.** HPX is started/stopped solely by an HPX-needing owner
  (`rayx.runtime.Runtime` / `rayx.Engine`) through the shared process-level owner (§4).
* The **endpoint→Runtime bridge is local structural plumbing** (§5): a cross-process
  endpoint request can deliver one *fixed registered* Runtime op into a live same-process
  `Runtime` and return a typed result.
* **This is not distributed fabric.** It is single-node, plain local IPC. There is no HPX
  socket serving, no HPX async socket I/O, no parcelport, no AGAS, no multi-node, no
  persistent transport, and no public endpoint-call API beyond the existing fixed ping.
  The future distributed-fabric direction stays gated on separate evidence and design (§8).

Ray's role throughout is **bootstrap / lifecycle only**: it creates the actor processes
and carries plain serializable endpoint metadata between them. Ray is the rendezvous, not
the data path, and is never timed here.

---

## 2. Endpoint identity seam

Two RayX actors on **one node**, two processes. Each hosts one `Endpoint`, which mints a
stable opaque id `rtb-ep-<16 lowercase hex>` and registers an `EndpointSeam` in a
**process-local registry**. Bootstrap flows through Ray: each actor returns its endpoint
**metadata** (plain serializable dict), and the driver hands each peer's metadata to the
other.

Public API:

* `Endpoint()` — mint id, register the seam (HPX-free; see §4).
* `Endpoint.id` — the stable `rtb-ep-…` identity.
* `Endpoint.metadata()` — the plain bootstrap dict (closed, flat; see §3 for the six keys).
* `Endpoint.close()` — unregister the seam (tombstone), release any transport share.
* `connect(peer_metadata) -> Connection` — validate + resolve the peer.
* `Connection.ping(nonce: int64) -> int64` — the fixed typed handshake.
* `Connection.close()`, and a module-level idempotent `shutdown()`.

**The handshake (peer-specific).** `ping` returns

```
response = nonce ^ ENDPOINT_PING_XOR ^ endpoint_id_hash(peer_id)
```

where `ENDPOINT_PING_XOR = 0x52415958` ("RAYX") and `endpoint_id_hash` is FNV-1a 64-bit
over the endpoint id, reinterpreted to a signed `int64` identically in C++
(`endpoint_seam.hpp`) and Python (`endpoint/_validate.py`). Because the response mixes the
**resolved peer's** id hash, pinging B vs C with the same nonce yields different valid
responses — structural evidence the call is parameterized by the resolved peer. It is
**not** cryptographic proof of dispatch and **not** security (the id is public, so the
value is recomputable); it is `int64`-only, with no payloads. The transform is computed
**inline** on both the same-process and cross-process paths — there is no HPX call in the
ping data path.

**Same-process registry path.** When `pid`+`host` match the current process and the id is
live, `connect` returns a local `Connection` resolved through the registry and the ping is
computed inline — no socket.

**Closed / tombstone behavior.** The registry keeps a process-lifetime **tombstone set**
of closed ids and exposes a three-state lookup — `LIVE` / `CLOSED` / `ABSENT` — so a
previously valid id that has been closed (while another endpoint keeps the listener alive)
reads as *closed*, not *not-found*. Tombstones are cleared at `shutdown()`.

**Typed error taxonomy (deterministic, bounded — never crash/hang):**
`EndpointValidationError` (bad token/metadata/nonce), `EndpointProtocolError`
(incompatible `proto_version`), `EndpointUnreachableError` (peer in another process with
no reachable transport, or another node), `EndpointNotFoundError` (listener up, id not in
the registry), `EndpointClosedError` (closed endpoint/connection or a peer that closed),
`EndpointTimeoutError` (bounded deadline exceeded). Two further errors are added by the
bridge (§5): `EndpointRuntimeUnavailableError`, `EndpointOperationError`.

---

## 3. Local endpoint transport (A1)

`Endpoint(transport=True)` (default **off**) enables an opt-in cross-process delivery path
for the fixed typed `int64` ping over **plain native AF_UNIX IPC**. With transport off, a
cross-process peer is reported cleanly as `EndpointUnreachableError` (the identity-seam
behavior).

* **One process-local listener**, shared (refcounted) by all `transport=True` endpoints —
  one AF_UNIX socket file per process (`ep-<pid>.sock`), one dedicated OS accept thread,
  started lazily on the first transport endpoint and torn down when the last closes.
  Incoming requests route by the `target_endpoint_id` in the frame against the existing
  process-local registry.
* **Owner-only socket dir.** Default `/tmp/rayx-ep` (override `RAYX_ENDPOINT_SOCK_DIR`),
  created/validated as a `0700` directory owned by the current uid; loose bits are
  tightened and re-verified, else construction fails with a typed error. (macOS `$TMPDIR`
  is avoided — its long path can exceed the AF_UNIX `sun_path` limit.)
* **One-shot dial-per-call.** A remote `Connection` is a *probed* handle: `connect()`
  probes reachability once, the handle owns **no persistent fd**, and each `ping()`
  performs **one bounded, one-shot AF_UNIX round trip** (dial → send → recv → close) under
  a single deadline covering connect+send+recv.
* **PING frame shape (fixed-size, big-endian).** Request **39 bytes**
  (`magic(4)"RAYX"` + `proto(2)=2` + `msg(1)=PING` + `rsv(1)` + `target_id(23)` +
  `nonce(8)`); response **16 bytes** (`magic(4)` + `proto(2)` + `status(1)` + `rsv(1)` +
  `value(8)`). No variable-length payloads, no blobs, no Python callbacks, no `ObjectRef`.
  The frame carries `target_endpoint_id`, so a stale/wrong id (`NOT_FOUND`) is
  distinguishable from nothing listening (`EndpointUnreachableError`).
* **Metadata (closed, flat — six keys):** `{endpoint_id, pid, host, proto_version,
  transport_kind, transport_addr}`. `host` is the same-node check (`host != gethostname()`
  → `EndpointUnreachableError`); `transport_kind ∈ {"none","unix","tcp"}` with `"unix"`
  the implemented path and TCP loopback documented only as a fallback shape.

**Invariance property (the central acceptance signal):** because the response is computed
from the served (target) seam's id hash, the cross-process result **equals** the
same-peer local-path result for the same peer and nonce. `connect(B).ping(n)` returns the
same value whether B is in-process or in a sibling process.

Robustness hardening folded in: closed-vs-stale tombstone distinction (§2), SIGPIPE-safe
writes (`send()` with `MSG_NOSIGNAL` / `SO_NOSIGPIPE` where available), socket-dir
permission enforcement, a response `proto_version` check, and the one-shot probed-handle
semantics above.

**Explicitly:** A1 is **not** HPX socket serving, **not** HPX async socket I/O — HPX is
not the delivery mechanism (plain native IPC, ping computed inline; the accept thread is a
plain OS thread). The honest "HPX serving / HPX async I/O" language is reserved for a
future serving slice and is not claimed here.

---

## 4. Shared owner model (Variant 2)

One process can hold both an `Endpoint` and a `Runtime`. A single process-level **HPX
owner** (`ProcessHpxOwner`, a mutex-guarded singleton) applies a coexistence policy:

* **`Engine` — exclusive.** Attaches only when nothing else is attached; blocks all other
  attaches while live. Keeps the synthetic benchmark harness deliberately separate.
* **`Runtime` — process-singleton, HPX owner.** At most one; owns HPX while it lives; may
  coexist with endpoints; not allowed alongside `Engine`.
* **`Endpoint` — multiple, HPX-free.** Joins the owner's *policy* but requests **0 HPX
  workers** and does **not** itself start HPX; coexists with a `Runtime` and other
  endpoints; rejected only while an `Engine` is active.

So the only HPX-needing "primary" at any moment is **`Runtime` XOR `Engine`**, plus
zero-or-more endpoints. Consequences (Variant 2):

* **The Runtime's requested thread count is authoritative.** Endpoint contributes no
  thread constraint, so endpoint-first → Runtime never conflicts and the thread-topology
  ordering hazard disappears (HPX thread count and pools are fixed at `hpx::start`).
* **An `Endpoint` never starts or stops HPX.** HPX starts on the first HPX-needing attach
  and stops on the last HPX-needing detach. A live endpoint does not keep HPX up.
* **Teardown is order-independent.** Close the endpoint first → the Runtime still works;
  shut the Runtime first → the endpoint still works (its ping is HPX-free). Each subsystem
  drains its own resources (the endpoint stops its listener and unlinks its socket; the
  Runtime cancels/drains/joins its actor lanes then op lanes) before detaching; the owner
  clears state last.
* **`_process_hpx_active()` is a diagnostic / control gate only** — it reports whether the
  process HPX runtime is up (owned by a `Runtime`/`Engine`). An endpoint-only process
  reports `False`. It is not scheduler state and not a synchronization primitive.

Synchronization: one `owner_mu` mutex guards the attach/detach decisions and the HPX
start/stop refcount; the CPython GIL serializes the Python-side construction/teardown
calls. No free-threaded / no-GIL behavior is assumed or claimed. No named/IO pools and no
`async_scope` are introduced by this model.

---

## 5. Endpoint → Runtime bridge

The bridge is the first slice where a cross-process endpoint request does **real Runtime
work** rather than the inline ping. It is a **private, test-only** seam.

**API surface (smallest).** A private `Connection._call_op(op_id, *args)` — underscore
prefixed, **not** in `__all__`, used only by smoke/integration tests. `Connection.ping`
and the public surface are unchanged. There is **no public endpoint-call API.**

**Native I/O stays native.** AF_UNIX socket I/O is unchanged from A1. The **accept thread
is a plain `std::thread`**, never an HPX worker; the whole wire path (accept → read → …
→ write → close) runs on it. HPX is entered **only behind the request**, solely to
dispatch the registered op.

**The HPX-entry hop is legitimate here (not theater).** Unlike the A1 ping (a pure XOR,
computed inline), the bridge handler enters real Runtime dispatch — enqueue onto an
`hpx::mutex`-guarded `RuntimeLane` queue, which must be locked from an HPX thread — via a
single `run_as_hpx_thread` hop that returns an `hpx::future<RuntimeResult>`. The accept
thread, being an **external OS thread (not an HPX worker)**, then blocks on `fut.get()`;
blocking it does not consume an HPX core, so the Runtime fulfills the future even at
`hpx_threads=1`. A `fut.then(write_response)` continuation is **rejected** — it would
write the socket from an HPX worker, i.e. "HPX serving the socket," the exact boundary the
bridge refuses to cross. The consequence is a **serial, one-in-flight** bridge (the
listener `serve_one` blocks), accepted as the cost of keeping socket I/O native.

**Drain gate (lifetime correctness).** The hazard is the accept thread enqueuing onto a
lane after `Runtime.shutdown()` cleared the lanes. A **process-global bridge slot guarded
by one mutex + condition_variable** holds `shared_ptr<RuntimeBridge> bridge_`, `bool
draining_`, and `int in_flight_` under the same mutex. The Runtime publishes the bridge
after its lanes are live and unpublishes/drains before clearing them; no bridge mutex is
held across `run_as_hpx_thread` or `future.get()`. A stress test issues bridge calls while
a Runtime shuts down and restarts, asserting every call yields a typed error or clean
completion — never a crash, hang, or use-after-free.

**Wire protocol (fixed, closed) — preserving A1.** The A1 `PING` frames stay byte-for-byte
unchanged; the bridge adds a parallel `CALL_OP` message (`WIRE_MSG_CALL_OP = 2`). The
op-code is a **closed enum** (no arbitrary op-id strings), typed arg slots are fixed
9 bytes (`tag(1)` + `value(8)`), the request is 53 bytes and the response 16 bytes. The
server maps wire op-code → registered op-id → existing registry. A new `INTERNAL_ERROR`
wire status plus the bridge statuses (`RUNTIME_ABSENT`, `RUNTIME_DRAINING`, `OP_UNKNOWN`,
`OP_BADARGS`, `OP_FAILED`) map to typed Python errors. A pybind-free
`RuntimeEngine::dispatch_op_typed(op_id, OpArgs)` is shared by `submit_operation` and the
bridge, so no Python object is ever created on the accept thread (it holds no GIL).

**Supported fixed ops:**

* **v1:** `OP_SQUARE = 1` → `square(int64) -> int64`, `OP_ADD = 2` →
  `add(int64, int64) -> int64`. These are **structural plumbing** — trivial arithmetic
  whose HPX scheduling is invisible. v1 proves the *seam*, not HPX value.
* **v2 (composed-op structural bridge):** `BRIDGE_OP_FANOUT_SUM = 3` →
  `fanout_sum(int64 n, int64 parts) -> int64`, via private `_call_op("fanout_sum", n,
  parts)`. No wire-frame change (it reuses the two `int64` arg slots). A bridge-side cap
  `BRIDGE_FANOUT_N_MAX = 1000000` bounds `n` (`n > cap` → `OP_BADARGS` →
  `EndpointValidationError`) purely as a shutdown/drain bound; invalid `n`/`parts` make the
  op body throw → `OP_FAILED` → `EndpointOperationError`.

**What v2 shows (and does not).** v2 proves the bridged path reaches a Runtime op body
running in a **valid HPX-thread context** and executes nested HPX composition
(`hpx::async` + `hpx::when_all`) correctly, returning the right typed result.
Parts-invariance (`fanout_sum(n,1) == fanout_sum(n,4) == fanout_sum(n,8)`) is a
**correctness/regression check, NOT HPX evidence** — a sequential split-sum would also
pass. v2 does **not** claim HPX scheduling value, overlap, parallelism, speedup,
throughput, latency, or performance. No arbitrary payloads, no Python callbacks.

---

## 6. Corrected path interpretation (exp42 / exp43)

Two observation-only experiment packages characterize the **local path shapes** around the
bridge. They are summarized here and detailed in their own notes
(`experiments/42_endpoint_bridge_boundary_cost/bridge_boundary_cost.md`,
`experiments/43_endpoint_transport_ping_floor/endpoint_ping_floor.md`).

A correction that shaped both: a **same-process** endpoint bridge does **not** cross
AF_UNIX. `connect()` branches on `meta["pid"] == os.getpid()`; same-pid metadata resolves
through the in-process registry and returns a local `Connection`, even with
`Endpoint(transport=True)`. Only a cross-process peer takes the AF_UNIX path.

**exp42 — boundary/orchestration characterization (observation-only):**

* **P0 = direct Runtime submit:** `Runtime.submit_operation(...).result().value`.
* **P1 = same-process in-process bridge dispatch (no socket):** parent owns Runtime +
  `Endpoint(transport=True)`, connects to its own metadata (same pid → registry
  short-circuit), and `_call_op(...)` dispatches through the same-process drain-gated
  bridge — **no AF_UNIX, no accept thread, no frame codec.**
* **P2 = cross-process AF_UNIX bridge into a child Runtime:** a child process owns its own
  Runtime + transport endpoint; the parent connects to the child metadata (pid ≠ getpid →
  AF_UNIX) and bridges into the child.
* Interpretation: `P1 − P0` is the **in-process bridge-dispatch path difference** (bridge
  marshalling + drain gate vs direct submit/RuntimeFuture retirement) — **not** socket
  cost. `P2 − P1` bundles AF_UNIX delivery, the accept-thread hop, response framing, a
  second process, and a **second live HPX runtime** — it does **not** isolate transport.

**exp43 — runtime-less endpoint ping floor (observation-only):**

* **EP0 = same-process inline endpoint ping** (registry path; no socket, no Runtime).
* **EP1 = cross-process one-shot dial-per-call endpoint ping** — child is a transport
  `Endpoint` **only** (no Runtime); both sides are HPX-free and Runtime-free
  (`_process_hpx_active()` is false on both).
* **EPraw = a same-shape *Python* AF_UNIX echo control.** It is **not** a raw OS lower
  floor and does **not** isolate kernel AF_UNIX cost: its interpreted-Python server is not
  comparable to EP1's native accept thread, so `EP1 − EPraw` is a cross-implementation
  observation only (its sign carries no rayx meaning).

**Do not mechanically subtract `P2 − EP1`.** exp42's P2 child runs a live HPX Runtime with
worker threads contending with its accept thread; exp43's EP1 child has no HPX workers.
EP1 is a runtime-less *lower reference* for the one-shot endpoint ping envelope, not a
clean subtractor for P2's Runtime-dispatch cost. All exp42/exp43 timings are observation-
only and machine-specific (OS-local, non-transferable).

---

## 7. Claims and non-claims

**Allowed claims:**

* Endpoint **identity** and **local transport** are structurally implemented (minted ids,
  process-local registry, peer-specific typed `int64` ping; one-shot AF_UNIX delivery with
  the invariance property).
* The **endpoint→Runtime bridge** can deliver fixed test calls (`square`, `add`,
  `fanout_sum`) into a live same-process `Runtime` and return a typed result.
* A bridged call can reach a **composed HPX-native op body** running in a valid HPX-thread
  context (v2 `fanout_sum`: `hpx::async` + `hpx::when_all`, correct typed result).
* exp42 / exp43 **characterize local observation-only path shapes** around the bridge and
  the runtime-less endpoint ping envelope.

**Required non-claims:**

* no distributed fabric;
* no parcelport; no AGAS; no multi-node;
* no persistent transport / channel claim;
* no HPX socket serving; no HPX async socket I/O (HPX is not the delivery mechanism — plain
  native IPC, ping computed inline, accept thread is a plain OS thread);
* no public endpoint-call API (the bridge is private/test-only);
* no arbitrary payloads; no Python callbacks;
* no `ObjectRef` / object-store semantics;
* no Ray comparison;
* no speedup / throughput / latency claim;
* no exact transport-cost decomposition (P1−P0 is not socket cost; P2−P1 is not isolated
  transport; EPraw is not a raw OS floor; no mechanical P2−EP1 subtraction);
* no HPX-value / scheduling / overlap / parallelism claim from the bridge or v2
  parts-invariance.

---

## 8. Relationship to the future distributed-fabric direction

Kept short and fenced. The endpoint work gives **local identity and local bridge plumbing
only**:

* it proves endpoint discovery and a private endpoint→Runtime delivery seam on **one node**
  over plain AF_UNIX IPC;
* it does **not** pull future distributed-fabric work forward, and the endpoint/IPC
  observations (exp42/exp43) are **not** fabric evidence;
* the future distributed-fabric direction (HPX-managed async serving, then any
  parcelport / AGAS / locality-to-locality path, then multi-node) remains **gated** on
  separate evidence and design. HPX's distributed runtime assumes a fixed locality set and
  a coordinated launch with the parcelport owning the network endpoints — an architectural
  mismatch with independently launched Ray actors each embedding a single-locality HPX
  start — so it is deferred and is not constrained by A1's throwaway wire codec.

---

*The four prior notes (linked at the top) are now pointer stubs. Their per-slice
validation runs, file changelists, and the superseded either/or HPX-ownership provenance
were working-tree-only design notes and are intentionally dropped in this consolidation;
the durable design is captured above, and experiment-level detail lives under
`experiments/`.*
