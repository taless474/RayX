# HPX-Native Lane-Primitive Feasibility Probe

An **isolated primitive probe**, not a serving-control benchmark. It measures two
low-level HPX/std primitives in isolation so we can reason about *why* the
serving-control results behave as they do. It does **not** produce benchmark
JSONL, does **not** use retire modes, lanes, Ray, or the `rayx` Python frontend,
and its numbers are **not comparable** to benchmark 06/10 or any other entry in
the benchmark/experiment corpus.

`hpx_impl/service_lane.hpp` is used **unmodified**.

## 1. What this probe measures (and only this)

1. **Sleep overshoot** — how much longer than requested a sleep actually parks,
   for 1 / 5 / 20 ms targets, comparing:
   * `std::this_thread::sleep_for` (blocking; what the service lane actually uses)
   * `hpx::this_thread::sleep_for` (cooperative; yields the HPX worker)
2. **No-op dispatch throughput** — ops/sec for:
   * the **current `ServiceLane` reference path** (single serialized FIFO lane,
     `service_ms = 0`, i.e. pure dispatch with no synthetic work) — the
     actor-like anchor;
   * a **plain `hpx::async` no-op path** (one trivial task per op).

   These two are **not** a serving-lane-vs-serving-lane comparison. The lane is a
   single serialized FIFO consumer; `hpx::async` is the HPX task scheduler
   spreading trivial tasks across worker threads — **scheduler territory, not a
   serving-lane result**. The numbers are reported side by side only as a
   primitive contrast and must not be read as one engine being "faster" at
   serving.

## 2. How to run

```text
# build (both hpx_impl targets)
cmake --build hpx_impl/build

# full probe (writes aggregate.json beside this report)
python experiments/15_hpx_native_lane_feasibility/run_lane_feasibility.py

# laptop smoke (no aggregate.json written)
python experiments/15_hpx_native_lane_feasibility/run_lane_feasibility.py --quick
```

The probe binary (`hpx_impl/build/hpx_lane_feasibility`) emits a compact
`lane-feasibility-1` JSON summary to a scratch path under `results/`
(gitignored). The runner validates that JSON with loose shape gates
(presence + sane sign/range, **not** exact timing) and writes the curated
`aggregate.json`. Magnitudes below are macOS-laptop-specific.

Config: `hpx_threads=4`, `ops=50000`, `repeats=5`, `sleep_samples=200`.

## 3. Sleep overshoot

Median / p99 of 200 samples per cell (full run):

| primitive | target | observed p50 | observed p99 | p50 overshoot | p99 overshoot |
|---|---|---|---|---|---|
| `std::this_thread::sleep_for` | 1 ms | 1.265 ms | 1.277 ms | +26.5% | +27.7% |
| `std::this_thread::sleep_for` | 5 ms | 6.266 ms | 6.294 ms | +25.3% | +25.9% |
| `std::this_thread::sleep_for` | 20 ms | 25.013 ms | 25.063 ms | +25.1% | +25.3% |
| `hpx::this_thread::sleep_for` | 1 ms | 1.137 ms | 1.149 ms | +13.7% | +14.9% |
| `hpx::this_thread::sleep_for` | 5 ms | 5.635 ms | 5.705 ms | +12.7% | +14.1% |
| `hpx::this_thread::sleep_for` | 20 ms | 21.032 ms | 21.091 ms | +5.2% | +5.5% |

**Measured facts.**
* The blocking `std::this_thread::sleep_for` carries a stable, roughly
  **proportional ~25% overshoot** across 1/5/20 ms — matching the
  sleep-fidelity inflation already documented in
  `experiments/01_sleep_overshoot/sleep_overshoot_note.md` for the HPX-native
  and `rayx` paths (both of which sleep via this primitive in the lane).
* The cooperative `hpx::this_thread::sleep_for` is **tighter**: ~13–14% at 1 ms
  but only **~5% at 20 ms** — close to the Ray `time.sleep` behavior recorded in
  experiment 01 (~5% at 20 ms).

**Interpretation (separate from the facts above).** This isolates the
sleep-primitive overshoot as a property of the *blocking* `sleep_for`
specifically; the cooperative HPX timer wakes more tightly here. This is a
primitive-fidelity observation on this machine, **not** a control-plane result
and **not** a reason to change the lane: the lane deliberately uses the blocking
sleep so a single lane stays occupied and queueing builds up like a single
actor (a cooperative yield would break single-lane serialization — see the
comment in `service_lane.hpp`). The probe explains the ~25% caveat; it does not
motivate swapping the primitive.

## 4. No-op dispatch throughput

Median of 5 repeats, 50 000 ops each (full run):

| path | median throughput | median ns/op |
|---|---|---|
| `service_lane` (single serialized FIFO lane, `service_ms=0`) | ~4.67 M ops/s | ~214 ns/op |
| `hpx_async` (trivial task per op, scheduler) | ~4.08 M ops/s | ~245 ns/op |

**Measured facts.** Both primitives sustain **millions of no-op ops/sec** on
this laptop; per-op dispatch cost is a couple hundred nanoseconds in each case.
Run-to-run ordering between the two is noisy (in the `--quick` smoke the
`hpx_async` path was nominally ahead; in the full run the lane path was) — they
are the same order of magnitude.

**Interpretation (kept deliberately narrow).**
* The single serialized lane's pure-dispatch floor is far below any synthetic
  service time used in the corpus (1–20 ms), so dispatch cost is **not** the
  thing the serving-control benchmarks are measuring — service time and
  client-driven FIFO-retire behavior dominate there.
* The `hpx_async` number is **scheduler territory**, included only as a
  primitive contrast. It is **not** a serving-lane result: it has no single-lane
  serialization, no actor-like ordering, and no client-driver loop. Do not
  compare it to lane throughput as if one "wins" at serving.
* `ServiceLane` remains the **stable actor-like anchor** for the project; this
  probe does not change that.

## 5. Scope and caveats

* **Isolated primitive probe.** Not comparable to benchmark 06/10 or any other
  corpus entry. No JSONL schema, no retire modes, no Ray, no `rayx` frontend.
* **Machine-specific.** macOS laptop, 10 cores (4 P + 6 E), single locality.
  Sleep overshoot in particular is OS/timer-specific and will not transfer.
* **Synthetic only.** `service_ms=0` here is pure dispatch; sleeps are synthetic
  parking, not real work. Nothing here is model inference.
* **spin is unrelated to this probe.** Spin stays what it is elsewhere — a
  CPU-bound diagnostic / calibration axis — and is intentionally out of scope
  here (this probe is about sleep-primitive fidelity and no-op dispatch only).
* `service_lane.hpp` was not modified; the `rayx` public API, drivers, analyzer,
  and JSONL schema are untouched.

## 6. Possible follow-up (only if these v1 results motivate it)

If a future question needs it, a serialized-chain / single-worker-executor
dispatch variant could be added as a third primitive contrast (e.g. chaining
no-ops on one HPX executor) to separate scheduler spread from serialized
dispatch more directly. **Not implemented here** — v1 deliberately measures only
the current `ServiceLane` reference path and plain `hpx::async`. This bullet is a
note, not a commitment.
