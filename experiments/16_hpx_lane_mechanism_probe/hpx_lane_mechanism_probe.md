# HPX Cooperative Lane — Mechanism Probe (std lane vs HPX-thread lane)

An **opt-in, explicitly-incomparable lane-mechanism probe**. It compares the
native baseline's two serialized-lane implementations under an otherwise
identical single-lane workload, to answer one question: **what changes if a
serialized lane uses HPX-native scheduling/timer primitives while preserving
actor-like FIFO semantics?**

This is **not** comparable to benchmark 06/10 or any prior package: those hold
the lane mechanism fixed (always `ServiceLane`); this one **varies** it. The
HPX-lane rows carry a distinct `boundary` (`hpx-intra-locality-hpxlane`) and an
`_hpxlane` workload tag so they never silently fold into the corpus.

`HpxLane` is **not** a replacement for `ServiceLane` (which remains the stable
Ray-actor-like anchor), and this is **not** a general HPX-scheduler result — it
is one serialized FIFO lane whose timer/suspension primitives are HPX-native.

## 1. The two lanes

| | `--lane-impl std` (anchor) | `--lane-impl hpx` (cooperative) |
|---|---|---|
| type | `rayhpx::ServiceLane` | `rayhpx::HpxLane` |
| consumer | one `std::thread` | one `hpx::thread` |
| queue suspension | `std::mutex` + `std::condition_variable` | `hpx::mutex` + `hpx::condition_variable_any` |
| parked sleep | **blocking** `std::this_thread::sleep_for` | **cooperative** `hpx::this_thread::sleep_for` (yields the HPX worker) |
| spin | busy on-core (unchanged) | busy on-core (identical) |
| FIFO | single consumer, submission order | single consumer, submission order |
| `actor_id` prefix | `act-hpx-` | `act-hpxl-` |

The **only** deliberate differences are the consumer thread type, the queue
suspension primitive, and the parked-sleep timer. `service_lane.hpp` is
unchanged; `HpxLane` reuses `rayhpx::Request` / `rayhpx::Result`. In **this
native single-lane probe** the `HpxLane` worker is driven only through its
token-less native `submit` path: the native driver never cancels, so cancellation
is "not applicable" here and the chunked service body is the same shape minus the
token-boundary checks. (Cancellation is **not** intrinsically absent from
`HpxLane`: the rayx backend `lane_impl="hpx"` does wire the shared
`rayhpx::CancelToken` into `HpxLane` — queued and chunk-boundary running
cancellation — exercised in exp21 (parity) and exp22 (load divergence). That
rayx-backend path is simply out of scope for this native mechanism probe.)

## 2. How to run

```text
# build (both hpx_impl targets)
cmake --build hpx_impl/build

# native quick check, each lane impl
hpx_impl/build/hpx_synthetic_baseline --hpx:threads=4 --lane-impl std --service-ms 5 \
    --num-lanes 1 --concurrency 1 --requests 20 --warmup-requests 2 \
    --retire-mode one_by_one --work-mode sleep --out results/std.jsonl
hpx_impl/build/hpx_synthetic_baseline --hpx:threads=4 --lane-impl hpx --service-ms 5 \
    --num-lanes 1 --concurrency 1 --requests 20 --warmup-requests 2 \
    --retire-mode one_by_one --work-mode sleep --out results/hpx.jsonl

# full experiment (writes aggregate.json beside this report)
python experiments/16_hpx_lane_mechanism_probe/run_lane_mechanism.py
# laptop smoke (no aggregate.json written)
python experiments/16_hpx_lane_mechanism_probe/run_lane_mechanism.py --quick
```

`--lane-impl` defaults to `std`, so every pre-existing invocation is byte-identical.

## 3. Matrix

`lane_impl {std, hpx}` × `service_ms {0, 1, 5, 20}` ms, `work_mode=sleep`,
`retire one_by_one`, `num_lanes=1`, `concurrency=1`, `hpx_threads=4`,
5 repeats × 200 requests (warmup 20). 40 runs. **Spin is excluded** — cooperative
timing has no effect on a busy-wait, so it would add noise without testing the
hypothesis (spin stays a CPU-bound diagnostic/calibration axis elsewhere).

## 4. Results (median of 5 repeats; macOS-laptop-specific)

| service_ms | lane | `service_ms_p50` | p50 overshoot | throughput (req/s) |
|---|---|---|---|---|
| 0 (no-op) | std | 0.000 | — | ~184 000 |
| 0 (no-op) | hpx | 0.000 | — | ~710 000 |
| 1 | std | 1.263 | **+26.3%** | ~792 |
| 1 | hpx | 1.138 | **+13.8%** | ~880 |
| 5 | std | 5.830 | **+16.6%** | ~169 |
| 5 | hpx | 5.658 | **+13.2%** | ~178 |
| 20 | std | 22.676 | **+13.4%** | ~43 |
| 20 | hpx | 21.046 | **+5.2%** | ~48 |

**Measured facts.**
* **FIFO/actor identity held** for both lanes: exactly one `actor_id` per run
  (correct prefix), 200/200 `completed`, schema `1`, correct per-impl
  `boundary`, no `failed`/`cancelled` rows — across all 40 runs.
* The **HPX cooperative lane has lower sleep overshoot at every service time**,
  strongly at 1 ms (26.3%→13.8%, ~48% relative) and 20 ms (13.4%→5.2%, ~61%
  relative), modestly at 5 ms (16.6%→13.2%, ~21% relative).
* Lower per-request overshoot shows up directly as **slightly higher
  one-by-one throughput** at the same target (e.g. 20 ms: ~48 vs ~43 req/s),
  which is expected at concurrency 1 (throughput ≈ 1000 / `service_ms_observed`).
* The `service_ms=0` no-op cell shows the cooperative lane with higher
  dispatch throughput, but this is a **no-op dispatch** datapoint, not a serving
  result — do not read it as a serving-control claim.

**Caveat on the std lane's own profile.** The std lane's in-lane overshoot here
(26% → 17% → 13% as duration grows) is **not** the flat ~25% the *isolated*
`std::this_thread::sleep_for` primitive showed in experiment 15. The lane context
(separate consumer thread, dequeue + promise-set around each sleep) changes the
std profile too. So experiment 16 is its **own** measurement of two real lanes,
not a re-run of the experiment-15 primitive numbers — read the two side by side,
not as identical.

## 5. Go / no-go

**GO on the mechanism question.** Both design-§5 conditions hold:

1. **FIFO/structural (hard gates):** PASS — single `actor_id` per impl, all
   completed, schema 1, correct boundary/prefix, 40/40 runs; no scheduler-
   starvation, hang, shutdown, or future-ownership defects observed (clean drain
   and completion every run).
2. **Overshoot prediction (direction, design §5):** PASS — the cooperative lane's
   p50 overshoot is **strictly lower than the std lane at every assessed service
   time** (5 ms and 20 ms), same direction as experiment 15, reproducible across
   the two full runs taken.

The relative *magnitude* is service-time-dependent (large at 1/20 ms, modest at
5 ms), so the verdict is a **qualified GO**: the experiment-15 cooperative-timer
advantage **survives in a real FIFO lane**, but its size varies with duration and
is machine-specific. This justifies keeping `HpxLane` as a documented opt-in
mechanism axis; it does **not** justify changing `ServiceLane` or any corpus
interpretation.

## 6. Scope and non-comparability caveats

* **Opt-in, incomparable.** `--lane-impl` defaults to `std`; the corpus is
  unchanged. HPX-lane rows are tagged (`boundary`, workload) so they never merge
  into benchmark 06/10 or any prior package — which varied *boundaries/retire
  modes/lanes* but always used `ServiceLane`.
* **Not a replacement / not a scheduler result.** `ServiceLane` stays the
  anchor. This measures one serialized FIFO lane on HPX-native primitives, not
  the HPX task scheduler in general (that was the separate `hpx::async` contrast
  in experiment 15, also explicitly not a serving-lane result).
* **Sleep-only, synthetic, single-lane.** No spin, no multi-lane (cooperative
  sleep could let lanes interleave on fewer workers — deliberately out of scope),
  no real inference. Magnitudes are macOS-laptop-specific.
* **No schema/analyzer change.** Schema stays version `1`; the new boundary value
  flows through the analyzer's generic boundary note unchanged.

## 7. Possible follow-up (only if motivated)

A serialized-chain / single-worker-executor lane variant (the experiment-15
follow-up) could be added as a further mechanism contrast to separate scheduler
spread from serialized dispatch more directly. **Not implemented here** — this
package deliberately probes only the std lane vs the HPX cooperative lane. Note,
not a commitment.
