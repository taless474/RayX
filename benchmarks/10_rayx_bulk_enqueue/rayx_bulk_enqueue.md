# RayX Batch Lane Bulk-Enqueue — A/B

A small curated A/B for an **internal** RayX optimization: how a `submit_batch`
pushes its requests onto the service lanes. No public API change, no JSONL schema
change, no analyzer change.

> Across no-op (`service_ms=0`) batches, does grouping a batch's requests per lane
> and pushing each lane's group under **one lock + one notify** (instead of one
> lock+notify per request) cut producer-side enqueue cost and lift end-to-end
> batch throughput — without changing any observable behavior — and does the win
> grow with lane count?

**Synthetic timing only.** This is **not** an HPX #4703 scheduler-batching result,
**not** a Ray result, **not** real inference, and **not** a general workload
speedup. It is the RayX-side lane enqueue overhead on no-op/tiny batches;
magnitudes are machine-specific.

## 1. Setup

* **Boundary:** `hpx-python-frontend` (rayx batch submit, one Python→C++ crossing).
* **Method:** in-process A/B through the **internal, undocumented** diagnostics on
  the raw `_Engine` — `_set_bulk_enqueue(True|False)` selects the strategy and
  `_submit_batch_cost_probe(count)` returns the producer-side ns split. Throughput
  is end-to-end `submit_batch()+get()` wall-clock. This deliberately does **not**
  use the JSONL benchmark driver/analyzer: the bulk-vs-single difference is
  internal and **not observable** in the per-request JSONL (identical input order,
  round-robin `actor_id`, schema `1`), so the driver has nothing to A/B.
* **Matrix:** `service_ms=0`, lanes {1, 4, 8} × count {1k, 10k} × {single, bulk},
  + one sanity cell (lanes 4, count 200, `service_ms=5`). Producer ns = median of
  9 probes; throughput = median of 7 (3 for the sanity cell).
* **Machine:** macOS laptop, 10 cores (4 P + 6 E), single locality.
* **Reproduce:** `python benchmarks/10_rayx_bulk_enqueue/run_bulk_enqueue_ab.py`
  (`--quick` for a tiny subset). Curated evidence: `aggregate.json` + this note.

## 2. The two paths

* **Old — one-by-one.** `submit_batch` looped over requests, calling
  `ServiceLane::submit` per request: **one mutex lock + one `notify_one` per
  request**.
* **New — per-lane bulk (shipped default).** Requests are grouped per lane on the
  existing round-robin mapping `lane_i = (rr_start + i) % num_lanes`, each lane's
  group is pushed under **one lock + one `notify_one`** via the additive
  `ServiceLane::submit_bulk`, and the futures are scattered back to input order.
  The single-request `ServiceLane::submit` and the native baseline are untouched.

**Invariants preserved** (verified): input order, round-robin `actor_id`, the
shared Python `submit_ns`, scalar and varied (`service_ms=[...]`) forms, batch
chunking rejection, batch non-cancellation, and the result-row / JSONL schema
(`1`). Bulk vs single produce byte-identical `actor_id` sequences and varied-batch
ordering across 1/4/8 lanes.

## 3. Measured facts (medians)

Producer enqueue is ns **per request** (cost probe); throughput is end-to-end
req/s. From `aggregate.json`:

| lanes | count | enqueue/req single | enqueue/req bulk | enqueue speedup | thr single | thr bulk | thr gain |
|---|---|---|---|---|---|---|---|
| 1 | 1k | 150.2 ns | 43.2 ns | 3.5× | 876,840 | 1,020,538 | **+16.4%** |
| 1 | 10k | 159.2 ns | 39.2 ns | 4.1× | 859,774 | 968,374 | **+12.6%** |
| 4 | 1k | 520.8 ns | 67.8 ns | 7.7× | 632,011 | 937,464 | **+48.3%** |
| 4 | 10k | 242.4 ns | 57.3 ns | 4.2× | 767,909 | 809,741 | **+5.4%** |
| 8 | 1k | 1355.5 ns | 101.3 ns | 13.4× | 253,990 | 875,561 | **+244.7%** |
| 8 | 10k | 1245.2 ns | 66.2 ns | 18.8× | 447,083 | 948,654 | **+112.2%** |

**Sanity cell** (lanes 4, count 200, `service_ms=5`): single 628 vs bulk 647 req/s
(**+3.1%**, within noise) — once real service dominates, the enqueue saving is
irrelevant, as expected.

`pybind_wrap_ns_per_req` (also in `aggregate.json`) is ~40–100 ns/req and flat
across lanes/modes — the future-object construction the bulk change does **not**
touch.

## 4. Interpretation

1. **Producer enqueue improves a lot:** 3.5–4.1× at 1 lane, 4.2–7.7× at 4 lanes,
   13–19× at 8 lanes. Bulk collapses N lock acquisitions and N `notify_one` calls
   into one per lane per batch.
2. **End-to-end no-op batch throughput improves:** +12–16% (1 lane), +5–48% (4),
   **+112–245% (8)** — the producer saving survives to the wall clock; the feared
   producer/consumer-overlap loss did not materialize.
3. **Single-lane does not regress** — it improves modestly (+12–16%).
4. **Why the win grows at 4/8 lanes.** One client thread round-robins across the
   lanes. Single-mode pays N lock acquisitions spread over L hot mutexes and N
   `notify_one` calls; at higher L each lane sees ~N/L arrivals, so its consumer
   more often finds the queue empty and is asleep — turning each `notify_one` into
   a futex wake. Bulk reduces this to L locks + L notifies per batch, so the
   per-request lock/notify/futex cost it removes grows with lane count (single-mode
   enqueue/req rises 150→520→1355 ns as lanes go 1→4→8; bulk stays ~40–100 ns).
5. **It vanishes under real service** (§3 sanity cell): with `service_ms>0` the
   single serialized lane is the bottleneck, not enqueue.

## 5. Non-claims / caveats

* **Not an HPX #4703 scheduler result.** This is the RayX `std::thread` +
  `std::deque` service lane, **not** the HPX task scheduler queue (see
  `docs/reference/hpx_4703_bulk_task_ops_note.md`). It says nothing about HPX
  scheduler bulk task operations.
* **Not a Ray result, not real inference, not Ray Serve.** Synthetic timing only;
  `service_ms` is duration control, never a payload or token.
* **Not a general workload speedup.** The win is **no-op / tiny-batch enqueue
  overhead**; it disappears once per-request service dominates (§3).
* **Machine-specific magnitudes.** Single laptop (4 P + 6 E); the firm signals are
  **structural** — single-lane no regression, multi-lane gain growing with lane
  count, all observable behavior unchanged. The exact percentages are noisy
  (e.g. the 4-lane cells swing with host load) and not portable.
* **In-process measurement.** Throughput here is `submit_batch()+get()` wall-clock,
  not a driver run; it is the right lens for an enqueue-strategy A/B, not a
  cross-engine latency comparison.

## 6. Why the diagnostics are kept (temporarily)

`_submit_batch_cost_probe` and `_set_bulk_enqueue` (and the one-by-one branch the
toggle drives) are retained as **internal, undocumented** diagnostics — they are
the only harness that can reproduce this A/B and the producer-side attribution, at
the cost of one predictable branch in the enqueue path. They are kept until the
bulk path is locked in across a real benchmark run, after which the single branch
becomes dead code worth removing in a cleanup pass. They are **not** on the public
`Engine` / `SyntheticActor` facade.
