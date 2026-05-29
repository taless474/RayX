# CPU-Bound `work_mode=spin` and the Saturation-Knee Sweep

Documentation for the `work_mode=spin` synthetic mode and the follow-up
`spin_knee_sweep` validation. Two slices, no schema/analyzer/Ray/API change.
Companion to `experiments/01_sleep_overshoot/sleep_overshoot_note.md` (the sleep artifact this mode
sidesteps) and `experiments/02_variable_service_lane_sweep/variable_service_lane_sweep.md` / `experiments/04_hpx_native_multiclient/hpx_native_multiclient.md`.

## 1. Why `spin` was added

The existing synthetic backend only used blocking sleep. Sleep is the right
probe for parked-lane queueing/coordination, but it carries a stable
sleep/wakeup timer artifact: HPX/rayx `std::this_thread::sleep_for` overshoots a
target by ~25% (a 5 ms request observes ~6.26 ms), Ray's `time.sleep` by ~5%
(see `experiments/01_sleep_overshoot/sleep_overshoot_note.md`). That overshoot is a confound for any
cross-engine service/latency comparison.

`spin` is a CPU-bound alternative: the shared `ServiceLane` busy-waits on-core
(no yield) until the target wall-clock duration elapses, measured on the same
`steady_clock` the metrics use. It is implemented once in
`hpx_impl/service_lane.hpp`, so the native executable and the rayx Python
frontend run the identical lane code; the native and rayx drivers gained
`--work-mode spin`. `service_ms == 0` stays a no-op; `sleep` is unchanged.

## 2. First spin slice (lanes {1, 4, 8})

Bimodal-free fixed service, `service_ms` 1 and 5, concurrency 16, requests 500,
`--hpx:threads=4`, 3 repeats, HPX native + rayx.

* **Service fidelity:** `service_ms_p50` = exactly **1.0000 / 5.0000** for both
  engines — the ~25% sleep overshoot disappears under spin.
* **Throughput:** scaled cleanly 1→4 lanes, then **sub-linearly** 4→8
  (e.g. svc=5: native 798→1466 req/s; rayx 795→1584).
* That 4→8 dip *suggested* a knee near 4 lanes — possibly the 4 HPX worker
  threads. But sampling only {1, 4, 8} cannot distinguish "knee at 4" from
  "4 is still on the linear part and 8 is already past the knee." Hence the
  follow-up sweep.

## 3. What `spin_knee_sweep` corrected

Result directory: `results/spin_knee_sweep_20260530T045307Z/` (156 runs, all
gates passed: completed/unique-id integrity, and `|svc_p50 − service_ms| ≤ 1%`).
Lanes **2, 3, 4, 5, 6, 8**; `--hpx:threads` **4** (svc 1 & 5, 5 repeats) and
**8** (svc 5, 3 repeats). Reproduced the prior {4, 8}-lane numbers within ~2%
except the svc=1/8-lane native point, which is high-variance (see §4).

Per-lane efficiency (req/s per lane ÷ the 1000/service_ms ideal):

* **`--hpx:threads=4`:** both engines hold **~98–99% through 6 lanes**, and only
  drop at 8 (native svc=5: 98.8% at L6 → 91.6% at L8; rayx svc=5 stays ~99% even
  at L8). Throughput keeps scaling near-linearly **well past 4 lanes**, so the
  knee is **not** at the HPX worker count of 4.
* **`--hpx:threads=8` (svc=5):** raising the worker pool did **not** push the
  knee outward. It made things **worse** — native efficiency falls to 89% at L4
  and 76% at L8, with `service_ms_p99` inflating to ~12–14 ms (vs ~5 ms at/below
  saturation). More HPX workers plus the per-lane spinning threads simply
  oversubscribe the fixed compute (this box is 10 cores: 4 P + 6 E).

So the saturation is better explained as a **hardware/core-boundary effect**,
not an HPX worker-count knee. We classify the boundary; we do not assert the
exact scheduler mechanism.

## 4. Native vs rayx

* **Below saturation (`threads=4`, lanes 2–6):** native and rayx throughput
  match within ~1%, repeat ranges overlap.
* **Above saturation:** rayx can come out **ahead** of native — `threads=4`/L8
  ratio ~1.08–1.10; `threads=8`/svc=5 grows from ~1.12 (L4) to **~1.24 (L8)**,
  with non-overlapping repeat ranges.
* This is a **robust-sign but magnitude-noisy** oversubscription/scheduling
  effect: the direction (rayx ≥ native past the knee) is consistent across
  service times and worker counts, but native's repeat spread is wide there
  (e.g. threads=8/L4 throughput range [616–770]) and its `service_ms_p99` tail
  is large. It appears only in the oversubscribed regime.
* It is **not** evidence that rayx is faster than native HPX. The heavier path
  (Python frontend) is the faster one here, so this is an oversubscription
  artifact of the single-client native `std::thread` driver, not frontend cost.
  Within the core budget the two are indistinguishable.

## 5. Scoped takeaways

* `spin` removes the sleep-fidelity artifact: observed service tracks the
  requested wall-clock duration (`svc_p50` exactly 1.0/5.0 ms).
* For CPU-bound synthetic work **within the core budget**, rayx still tracks
  native HPX within noise.
* High-lane behavior is dominated by **hardware scheduling / oversubscription**,
  not Python frontend overhead. The earlier "~4-lane knee" reading was an
  under-sampling artifact; the real knee is near the physical-core boundary.
* `sleep` and `spin` are **complementary** synthetic modes, not a replacement:
  `sleep` probes parked-lane coordination and timer behavior; `spin` probes CPU
  saturation. Use the one that matches the question.

## 6. Caveats

* CPU-bound spin is synthetic, not model inference; it burns CPU and can cause
  thermal throttling (runs were kept short).
* Local macOS laptop, single locality, 4 P + 6 E cores; on-core spin is more
  scheduling-sensitive than parked sleep, and these numbers are machine-specific.
* Ray omitted from the spin slices (a Python CPU-bound reference would be a
  separate, explicitly caveated comparison).
* p99/tails are softer than medians; throughput and p50 are the firmer signals.
* No mechanism claim about the OS scheduler beyond the classification above.
