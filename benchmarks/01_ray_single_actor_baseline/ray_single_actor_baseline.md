# Ray Single-Actor Baseline — Results Note

A short, honest record of the first Ray actor baseline matrix. This is a
results note, not a paper. Numbers are laptop measurements with known variance
(see caveats).

## 1. Purpose

Measure the control-plane behavior of **one Ray actor** acting as a synthetic
serving backend, across small service times and concurrency levels.

* **Boundary:** `ray-actor-process` — every number includes Python, the Ray
  runtime, the process boundary, serialization, and IPC.
* **Setup:** one single-threaded Ray actor; synthetic **sleep** backend only;
  `retire_mode=one_by_one`; `warmup_requests=20`; `requests=200` per cell.
* Goal is to characterize overhead vs service-time sensitivity, not to prove
  HPX is faster (no HPX here).

## 2. Commands / output location

Each cell ran:

```bash
.venv/bin/python bench/run_ray_baseline.py --service-ms <svc> --concurrency <c> \
  --requests 200 --warmup-requests 20 --retire-mode one_by_one \
  --out <OUTDIR>/ray_<label>_c<c>.jsonl
.venv/bin/python bench/analyze_jsonl.py <out> --out <out>.summary.json
```

* Matrix: `service_ms ∈ {0, 1, 5, 20} × concurrency ∈ {1, 4, 8, 16}` = 16 cells.
* Output directory: `results/matrix_ray_single_actor_20260529T060030Z/`
* **All 16 cells passed** (200/200 completed, 0 failed each).

## 3. Key result

* **One Ray actor behaves like a single serialized server.**
* Concurrency **deepens the queue**; it does **not** make the actor process
  requests in parallel (the actor is single-threaded).
* For **tiny service times** (0–1 ms), Ray actor/process overhead **dominates**
  total latency.
* For **20 ms service**, service time dominates and throughput is **capped by
  the serialized actor** (~43 req/s, roughly 1 / per-call slot) regardless of
  concurrency.

## 4. Summary table

`ms/call = 1000 / throughput`. `svc_p50` = observed service; `qw_p50` =
`total − service` (non-service time, see caveats). All latencies in ms.

| svc_ms | conc | tput (r/s) | ms/call | tot_p50 | tot_p90 | tot_p99 | svc_p50 | qw_p50 |
|---|---|---|---|---|---|---|---|---|
| 0  | 1  | 97.4  | 10.27 | 9.22   | 13.00  | 38.25  | 0.002  | 9.22   |
| 0  | 4  | 134.4 | 7.44  | 28.58  | 40.39  | 54.35  | 0.002  | 28.58  |
| 0  | 8  | 226.8 | 4.41  | 28.29  | 57.34  | 76.27  | 0.001  | 28.28  |
| 0  | 16 | 160.2 | 6.24  | 69.12  | 194.48 | 281.50 | 0.002  | 69.12  |
| 1  | 1  | 92.7  | 10.79 | 10.35  | 12.54  | 16.52  | 1.151  | 9.20   |
| 1  | 4  | 129.9 | 7.70  | 29.26  | 41.62  | 70.55  | 1.150  | 28.11  |
| 1  | 8  | 131.0 | 7.63  | 60.59  | 91.40  | 188.86 | 1.149  | 59.44  |
| 1  | 16 | 134.9 | 7.42  | 107.61 | 233.61 | 321.77 | 1.150  | 106.46 |
| 5  | 1  | 65.6  | 15.24 | 14.05  | 18.29  | 26.33  | 5.651  | 8.56   |
| 5  | 4  | 110.2 | 9.07  | 36.23  | 47.20  | 59.15  | 5.646  | 30.69  |
| 5  | 8  | 123.6 | 8.09  | 62.05  | 87.40  | 190.00 | 5.642  | 56.40  |
| 5  | 16 | 114.2 | 8.76  | 116.56 | 247.18 | 482.18 | 5.641  | 111.07 |
| 20 | 1  | 34.4  | 29.08 | 28.36  | 30.54  | 34.58  | 21.023 | 7.70   |
| 20 | 4  | 42.8  | 23.39 | 93.06  | 97.00  | 106.69 | 20.941 | 72.35  |
| 20 | 8  | 43.6  | 22.94 | 183.30 | 188.66 | 227.53 | 21.019 | 162.73 |
| 20 | 16 | 42.8  | 23.39 | 373.23 | 377.43 | 403.50 | 21.024 | 352.33 |

## 5. Interpretation

* **service_ms 0 / 1 — overhead-dominated.** Observed service is ~0.002 ms and
  ~1.15 ms, but total latency is several ms; at concurrency 1 the ~9–10 ms is
  pure synchronous round-trip dispatch cost, not queueing.
* **service_ms 5 — transition zone.** Service (~5.6 ms) is comparable to
  per-call overhead; neither clearly dominates.
* **service_ms 20 — service-dominated.** Service (~21 ms) dwarfs the ~2 ms
  dispatch overhead; throughput is flat across concurrency.
* **Latency grows roughly linearly with concurrency for one actor.** Clearest
  at svc 20: `tot_p50` ≈ 28 / 93 / 183 / 373 ms for concurrency 1 / 4 / 8 / 16,
  i.e. about 1× / 4× / 8× / 16× of the ~23 ms per-call slot — the *k*-th
  in-flight request waits behind *k − 1* others on the serial actor.

## 6. Caveats

* **Single actor only**, single-threaded — ceilings and linear latency growth
  are properties of one serialized server.
* **Laptop, single run per cell** — meaningful run-to-run variance. (Example:
  noop c8 measured 227 req/s here vs 393 req/s in an earlier run of the same
  config.) Trust the **trends**, not the exact ms.
* **No-op numbers are noisy** in particular (tiny service, dominated by
  transient and scheduling effects).
* **`queue_wait_ms` means non-service time, not literal queue time** —
  especially at concurrency 1, where only one request is in flight and the
  value reflects round-trip dispatch overhead.
* **Boundary differs from the future HPX baseline.** This is the Ray
  process/Python/IPC/serialization boundary (`ray-actor-process`); the planned
  HPX baseline measures `hpx-intra-locality`, which is cheaper and not directly
  comparable without stating the boundary.
* **Not a multi-actor scaling result.** Nothing here speaks to how Ray scales
  across multiple actors or nodes.

## 7. Next possible experiments

* Repeat selected cells multiple times to **quantify variance** (e.g. medians ±
  spread instead of single runs).
* **Multi-actor scaling** — measure throughput/latency with N actors to
  separate per-actor serialization from aggregate capacity.
* The **future HPX intra-locality baseline**, compared against this note with
  the boundary difference stated explicitly.
