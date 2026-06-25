# exp42 — endpoint→Runtime bridge boundary/path characterization (observation-only)

## Purpose

Characterize the **local path differences** between three *real* API ways an existing
registered composed op (`fanout_sum(n, parts=4)`) is reached, end to end, all dispatching
into the **same fixed op body**:

| Path | What it exercises | Runtimes active |
|------|-------------------|-----------------|
| **P0** | direct `Runtime.submit_operation(...).result().value` | 1 |
| **P1** | same-process **in-process** bridge dispatch (no socket) | 1 |
| **P2** | cross-process **AF_UNIX** endpoint bridge into a child Runtime | 2 |

This is a boundary/path probe, **not** an HPX-mechanism, scheduling, overlap, parallelism,
transport, or performance experiment. Sequential single-in-flight only. Laptop timing is
**observation-only and is not evidence**.

## The same-pid short-circuit (critical correction)

An earlier framing called P1 a "same-process endpoint bridge **over AF_UNIX**." That is
false, and this experiment deliberately does **not** fake it.

In `python/src/rayx/endpoint/__init__.py`, `connect(peer_metadata)` branches on the peer
pid:

```python
if meta["pid"] == os.getpid():
    # Same process: resolve through the registry and compute inline (no socket).
    native = _endpoint_connect_local(meta["endpoint_id"])
    ...
    return Connection(native)
# Cross-process, same node: dispatch on the advertised transport (AF_UNIX).
```

So when an endpoint connects to **its own** metadata (`connect(self.metadata())`), the peer
pid equals the current pid and resolution goes through the **in-process registry** — even
with `Endpoint(transport=True)`. `Connection._call_op(...)` then dispatches through the
same-process drain-gated bridge into the live same-process `Runtime`. There is:

* **no** AF_UNIX socket,
* **no** accept thread,
* **no** frame encode/decode,
* **no** `struct.unpack` response path.

The cross-process `call_op_remote(...)` AF_UNIX path is taken **only** when `pid != getpid()`
(P2). The experiment uses this real behavior directly: **no pid is spoofed and no
lower-level transport helper is called to manufacture a path the API would not take.**

## Corrected decomposition

* **P0** — `rt.submit_operation("fanout_sum", n, 4).result().value`:
  Python/pybind submit → lane enqueue → `run_as_hpx_thread` → op body → `future.get` →
  `RuntimeFuture` retirement.

* **P1** — `Endpoint(transport=True)` in the **same** process as `rt`;
  `connect(ep.metadata())` (same pid → registry, no socket); `conn._call_op("fanout_sum",
  n, 4)`: pybind `_call_op` → arg marshalling → same-process drain gate → `bridge_dispatch`
  → `run_as_hpx_thread` → op body → `future.get` (GIL released while blocking). Same single
  Runtime as P0.

* **P2** — a **child process** owns its own `Runtime` + `Endpoint(transport=True)`; the
  parent `connect(child_meta)` (pid ≠ getpid → AF_UNIX); `conn._call_op("fanout_sum", n, 4)`:
  pybind `_call_op` → frame encode → AF_UNIX round-trip → child accept thread → child drain
  gate → bridge dispatch into the **child** Runtime → op body → response frame →
  decode/unpack. The parent Runtime stays alive, so two HPX runtimes are active on the
  machine.

### What the deltas mean (corrected)

* **P1 − P0** is the **in-process bridge-dispatch path difference** (bridge arg marshalling +
  drain gate + `bridge_dispatch` vs direct submit / `RuntimeFuture` retirement). It is
  **not** socket cost. Read it on the `square` carrier and only when it clears jitter.
* **P2 − P1** is reported as an **end-to-end cross-process path observation only**. It is
  structurally useful (it exercises the real cross-process endpoint→Runtime path), but its
  timing is heavily confounded by second-process scheduling, a **second HPX Runtime**, possible
  HPX worker-pool **oversubscription** (parent + child worker pools on one machine), the child
  accept thread, AF_UNIX delivery, and response framing. It does **not** isolate transport and
  is **not** socket cost.

### GIL / deadlock invariant (corrected)

P1 has **no** accept thread; the calling Python thread releases the GIL and blocks on the
native future path. The accept-thread / `future.get` invariant applies only **inside the
child** in P2, and is safe there because the bridged op body is pure native C++ that never
re-enters Python. Do **not** generalize this to Python-callback operations (there are none).

## Carriers: `square` is the primary boundary floor, `fanout_sum` is illustration

* **`square` is the PRIMARY pure-boundary timing carrier.** It is near-zero-work and its body
  runs **no** internal HPX `async`/`when_all`, so a `square` path delta is the cleanest
  available estimate of pure boundary/orchestration cost (Python/pybind + bridge marshalling +
  drain gate + injection), with no common-mode op cost layered in. The `square_boundary_primary`
  block in `aggregate.json` holds these deltas; **read them first.**
* **`fanout_sum` is the composed-op correctness carrier + an amortization illustration only.**
  Its body runs `hpx::async × parts + when_all().get()` on **every** measured call. That op
  cost is common-mode across P0/P1/P2 and cancels in the delta, but it makes the boundary
  signal noisier than `square`. `fanout_sum` rows verify the composed op survives all three
  paths and illustrate how a fixed boundary cost amortizes as `n` grows — they are **not** the
  primary boundary measurement.

## Significance: judge deltas against jitter, not the timer

A µs-level difference of two medians is only meaningful if it clears **both** the timer floor
and run-to-run jitter:

* **Empty-loop floor:** median of an empty `perf_counter_ns` delta loop (timer + loop
  resolution, ~tens of ns).
* **Per-path `square` floor:** `square(7)` per path = that path's near-zero-work fixed call
  cost (reported as fixed-cost context).
* **Pooled IQR:** for a delta `median(b) − median(a)`, `pooled_iqr = max(IQR(a), IQR(b))` —
  the conservative run-to-run variation of the two paths.
* **`interpretation_status`** for each delta:
  * `below_resolution` — `|delta| ≤ empty_loop_floor` (timer noise);
  * `within_jitter` — `|delta| ≤ pooled_iqr` → **not reported as signal**;
  * `above_jitter_observation_only` — clears both floors (P0/P1 in-process);
  * `end_to_end_observation_only` — a P2 delta above jitter, flagged as confounded
    end-to-end cost (see P2 note), **not** transport/socket cost;
  * `withheld` — the composed-op flatness check failed for that thread.

We do **not** use a path's absolute `square` cost as the *delta* floor — the path's fixed call
cost is exactly what the delta measures, so gating on it would be circular.

* **Flatness check (COARSE, secondary):** for `fanout_sum`, `P1−P0` should not scale with `n`.
  Flagged contaminated only when the trend is monotone-up **or** strongly rank-correlated
  (Spearman ρ ≥ 0.9) with `n` **and** the spread clears one minimal in-process bridged call.
  This is a coarse self-consistency check, **not** a strong test — IQR-significance above is
  the primary interpretation guard.
* **Amortization interpretation:** `applied_illustration_only` only when a fixed in-process
  boundary cost is itself distinguishable from jitter (the `square` `P1−P0` is
  `meaningful_above_jitter`) **and** the composed-op flatness is self-consistent; `weakened`
  when no boundary cost clears jitter; `withheld` when flatness failed.

## HPX idle-backoff caveat

Each call enters HPX via `hpx::run_as_hpx_thread` **just to enqueue** the task onto the
persistent lane worker; the Python (or accept) thread then blocks on `future.get()` with the
GIL released. Because this experiment is **sequential single-in-flight**, HPX workers can go
idle between calls, so each call may pay scheduler **wake-up / idle-backoff** latency. Measured
µs-level path differences may therefore include HPX idle-backoff jitter. No HPX `ini` tuning
(e.g. `hpx.max_idle_backoff_time`) is applied here — the timing is **observation-only and is
not a stable layer constant**, and IQR-significance is the guard against over-reading it.

## How to run

Laptop (writes `aggregate.json` beside this file):

```
PYTHONPATH=python/src python \
  experiments/42_endpoint_bridge_boundary_cost/run_bridge_boundary_cost.py
```

Quick structural smoke (tiny grid):

```
PYTHONPATH=python/src python \
  experiments/42_endpoint_bridge_boundary_cost/run_bridge_boundary_cost.py --smoke
```

Defaults: `--ns 0,100,1000,10000,100000`, `--hpx-threads 1,4`, `--reps 200`, `--warmup 30`,
GC disabled inside the measured loops, sequential single-in-flight. Override the socket dir
with `RAYX_ENDPOINT_SOCK_DIR` (default `/tmp/rayx-ep-exp42`).

## Result (laptop, observation-only — NOT evidence)

Structural gates **PASS** (all three paths return identical values to the P0 reference across
the full `n` sweep at both thread counts). On this `darwin` laptop, judging deltas against
pooled IQR:

* **Primary (`square`) boundary:** the in-process `P1 − P0` clears jitter
  (`above_jitter_observation_only`) but is **small and machine-specific** — on this laptop it
  is mildly **negative** (in-process bridge dispatch measured slightly *below* direct submit),
  which is exactly why no directional/performance claim is made. The cross-process `P2 − P1`
  is the largest term (tens of µs) and is classified `end_to_end_observation_only`.
* **Composed-op (`fanout_sum`) carrier:** `P1 − P0` is roughly flat in `n` (it does not scale
  with op work) but is **noisier and even sign-divergent from the `square` carrier** — a direct
  illustration of why `fanout_sum` is *not* the primary boundary instrument. It confirms the
  composed op survives all three paths and amortization (fixed boundary cost shrinking relative
  to op work as `n` grows) is an *illustration only*.
* `flatness_ok=true` (coarse), `noise_floor_ok=true`,
  `amortization_interpretation=applied_illustration_only` — meaning only that a fixed in-process
  boundary cost is distinguishable from jitter on the clean carrier and the coarse flatness
  check is self-consistent. This is **not** a performance claim, and the absolute numbers may
  include HPX idle-backoff jitter (see caveat).

See `aggregate.json` (`square_boundary_primary`, `fanout_deltas`, `diagnostics`, `caveats`) for
the full per-delta IQR classification.

## Claim

> exp42 characterizes local observation-only path differences between direct Runtime submit,
> same-process in-process bridge dispatch, and cross-process local endpoint bridge for an
> existing composed Runtime op.

## Non-claims

* no same-process socket (P1 is in-process registry dispatch, no AF_UNIX);
* no isolated socket-cost claim;
* no isolated transport-cost claim (`P2−P1` bundles many costs; it is end-to-end only);
* no HPX mechanism / scheduling / **design** conclusion;
* no HPX value;
* no speedup / throughput / general latency claim;
* no stable layer-constant claim — sequential single-in-flight timing may include HPX
  idle-worker wake-up/backoff jitter;
* deltas at/within the IQR jitter band are **not** interpreted as signal;
* no Ray comparison;
* no transport / fabric / parcelport / AGAS / multi-node;
* no public endpoint-call API (`_call_op` is private/test-only);
* laptop timing is observation-only, not evidence.
