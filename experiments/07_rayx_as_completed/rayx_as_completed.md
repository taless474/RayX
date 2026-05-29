# Experiment 07 — RayX as-completed retire reproduces the native batch_wait fix

## 1. Purpose

RayX already matched native **FIFO `one_by_one`** behavior within noise (see
[experiments/03](../03_rayx_multiclient_driver/rayx_multiclient_driver.md) and
[experiments/04](../04_hpx_native_multiclient/hpx_native_multiclient.md)). The
native `--diag` decomposition
([experiments/06](../06_diag_fifo_ceiling_analysis/diag_fifo_ceiling_analysis.md))
then proved the ~1390 req/s bimodal high-lane ceiling is a **closed-loop
FIFO-retire / client-driver** effect — and that native `batch_wait`
(as-completed retirement) lifts it ~+85% by removing the client completion-wait,
not by changing HPX or the work.

The open question that left: does the **same fix work through the RayX Python
frontend**, or does Python/GIL impose a separate structural ceiling? This
experiment answers it by adding a windowed as-completed retire path to RayX
(built on the new `Engine.wait`) and comparing it to RayX FIFO on the exact
bimodal ceiling cell.

## 2. API under test

New RayX surface exercised here:

* **`Future.ready()`** — non-blocking `is_ready()` poll; raises if the future was
  already retired. A building block / test hook, not a polling loop.
* **`Engine.wait(futures, num_returns=1) -> (ready, not_ready)`** — blocks until
  at least `num_returns` of the given futures are ready, then returns a partition
  of the **same** `Future` objects (so each keeps its Python-side `submit_ns`).
  `num_returns=1` gives wait-any semantics.
* **`SyntheticActor.wait(...)`** — thin forwarder to `Engine.wait`, for facade
  symmetry (not used by the benchmark driver, which uses `--api engine`).

**Blocks in C++/HPX with the GIL released.** `Engine.wait` moves the underlying
`hpx::future`s into a temporary vector, calls `hpx::wait_some(num_returns, …)`
under `py::gil_scoped_release`, then moves them back into the same `_Future`
objects. The client thread sleeps inside HPX until a real completion wakes it.

**Why this is not Python busy-polling.** A Python `while not f.ready(): ...` loop
would hold the GIL while spinning, burn a core, and — worse — reintroduce a
Python/GIL artifact into the exact benchmark designed to show the ceiling is
*not* GIL. `Engine.wait` instead blocks in HPX (no spin, GIL released), exactly
like the native `hpx::wait_any` in `dispatch_batch_wait`.

**Why `submit_batch` is not the same comparison.** `Engine.submit_batch` enqueues
all requests in one Python→C++ crossing with **no fixed in-flight window**
(`--concurrency` becomes inert) and one shared `submit_ns`, so its latency is
bulk/queue-shaped. The native `batch_wait` fix keeps the **fixed concurrency
window** and only changes *retire discipline*. To mirror it faithfully RayX must
hold the window and retire as-completed — which is what `Engine.wait` enables and
`submit_batch` cannot.

## 3. Workload

The documented bimodal ceiling cell (matches experiments 02/06), via
`bench/run_hpx_python_baseline.py`:

| field | value |
|---|---|
| api | engine |
| work mode | sleep |
| service pattern | bimodal |
| service low | 1 ms |
| service high | 20 ms |
| p_high | 0.1 |
| seed | 0 |
| num lanes | 16 |
| concurrency | 32 |
| requests | 1000 |
| warmup | 20 |
| hpx threads | 4 |
| repeats | 3 |

Modes: **`one_by_one`** (FIFO windowed retire) and **`batch_wait --wait-batch 8`**
(as-completed windowed retire via `Engine.wait`). Reproduce with
`run_rayx_as_completed.py` (raw JSONL goes to ignored
`results/_rayx_ascompleted/`; the curated reduction is `aggregate.json`).

## 4. Results

Per-repeat throughput (req/s):

| repeat | one_by_one | batch_wait |
|---|---|---|
| r1 | 1362.3 | 2557.1 |
| r2 | 1365.5 | 2528.8 |
| r3 | 1380.0 | 2523.9 |

Reduced (representative = median-throughput repeat):

| metric | one_by_one | batch_wait |
|---|---|---|
| throughput median (req/s) | **1365.5** | **2528.8** |
| total_ms p50 | 23.956 | 1.355 |
| total_ms p90 | 38.606 | 28.025 |
| total_ms p99 | 48.782 | 70.197 |
| service_ms p50 | ~1.25 | ~1.25 |

**Throughput lift: 1.852× (+85.2%).** Service p50 is unchanged (~1.25 ms in both),
confirming identical work — only retire discipline differs.

## 5. Comparison to native anchors

| | throughput | completion/total shape |
|---|---|---|
| native FIFO `one_by_one` | ~1384 req/s | completion p50 ~21.9 ms |
| native `batch_wait` / as-completed | ~2558 req/s | completion p50 ~0.003 ms |
| **RayX FIFO `one_by_one`** | **1365.5 req/s** | total p50 23.96 ms |
| **RayX `batch_wait`** | **2528.8 req/s** | total p50 1.355 ms |

RayX FIFO tracks native FIFO (and the earlier RayX FIFO ~1370 req/s); RayX
`batch_wait` lands on native `batch_wait` (~2558). **RayX reproduces the same
+85% lift through the Python frontend.**

## 6. Interpretation

* **The FIFO-retire fix works through RayX.** Switching only the retire discipline
  (FIFO → as-completed `Engine.wait`) reproduces native's +85% throughput lift.
* **Python/GIL does not add a separate structural ceiling.** RayX FIFO matches
  native FIFO, and RayX as-completed matches native as-completed; there is no
  extra Python-side ceiling between them.
* **The improvement comes from retire discipline, not service work.** `service_ms`
  p50 is ~1.25 ms in both modes; the lanes do the same work.
* **The total p50 collapse (23.96 → 1.355 ms) shows ready-but-unretired FIFO
  waiting is removed.** Under FIFO, short completed requests sit behind older
  in-flight 20 ms requests; as-completed retirement releases them immediately.
* **The p99 tail can rise** (48.78 → 70.20 ms) because the unavoidable 20 ms
  requests and built-up queue depth remain — head-of-line cost relocates to the
  tail. **Do not overread p99 as a regression:** the median collapses and
  throughput rises; read those, not the p99 ratio.

## 7. Caveats

* **Clock domains.** RayX `submit_ns`/`recv_ns` use Python `perf_counter_ns`
  (includes the pybind/GIL crossing); the native diagnostic uses C++
  `steady_clock`. Compare shapes/ratios, not raw native-vs-RayX absolute ms. The
  as-completed path uses one shared `recv_ns` per `Engine.wait` sweep to match
  native's per-sweep `recv_ns`.
* **Python version.** The local run used Python 3.11 (the venv the `_rayx`
  extension is built against); CI (`native-rayx-smoke`) builds Python 3.12.
* **Scope.** Sleep-mode, synthetic backend, single machine, single locality,
  `--hpx:threads=4` only.
* **`--wait-batch 8`** (native parity) was not swept.

## 8. Verdict — **SUPPORTS**

This supports the conclusion that the FIFO-retire / client-driver ceiling is
**fixable through RayX** using `Engine.wait`. RayX as-completed is now the fair
Python-frontend analog of native `batch_wait`: same fixed concurrency window,
same as-completed retirement, same ~+85% lift — with no separate Python/GIL
ceiling.
