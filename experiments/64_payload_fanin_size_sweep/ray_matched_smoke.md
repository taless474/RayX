# exp64 — Slice 2: Ray matched payload smoke

**Status:** mechanism-smoke evidence only. HPX-free (this is the Ray arm), closed-int64 scalar +
synthetic response payload. NOT the full payload ladder, and NOT a ratio / speedup / winner comparison.
`same_axis_comparison` stays **False**, locked until a later gated matched-ladder aggregate earns it
structurally. Fences (`speedup_computed`, `ratio_reported`, `arms_differenced`,
`placement_bands_differenced`) locked False.

Slice 1 established the HPX-only payload fanin smoke (`root_flat_gather_poll`, payload bytes returned to
Python, digest folded after timing). Slice 2 adds the **Ray arm** at the same Python caller boundary, so
exp64 now has a second, structurally-gated payload-fanin arm ready for the later matched ladder. This
report only establishes that the Ray arm exists and passes the payload-smoke gates at S=0 and S=262144;
it does **not** compare the two arms.

## What Slice 2 is

A Ray actor-coordinator payload fanin, measured at the **same Python caller boundary** as the HPX arm.
This is a Ray-PATTERN mapping, not HPX best practice. Topology mirrors exp62 Slice 4b:

* Ray head on the first node with `num_cpus=0` (control-plane only);
* a coordinator hard-pinned to the head node with `num_cpus=0` that runs **zero** leaves;
* two remote worker nodes;
* N=8 leaves hard-pinned round-robin across the two remote worker node ids
  (`NodeAffinitySchedulingStrategy(soft=False)`), 4/4, all leaves remote.

One blocking call per timed iteration: `ray.get(coordinator.remote(x, n, payload_bytes))`. The
coordinator gathers the leaves and returns the **raw payload bytes** (plus scalar values and node
witnesses) to Python. Python stops the RTT clock when the bytes return, then folds and checks the scalar
oracle and payload digest **after** timing, **outside** the RTT window — the identical fold location to
the HPX arm. The coordinator **never** folds the digest inside the timed call; it is control-plane only.

## Local validation

* `python3 -m py_compile run_exp64_payload.py selftest_slice0.py` → OK
* `python3 selftest_slice0.py` → all 75 checks pass
* `python3 run_exp64_payload.py --phase selftest` → rc=0
* `--phase ray-payload-remote-smoke` skips cleanly off-cluster (needs ≥3 nodes + Ray)
* `git diff --check` → clean

## Operational note (Ray-on-Slurm)

The first hardware attempt, with the driver step under **default Slurm CPU binding**, failed during Ray
**GCS startup**: the nested `ray start --head` timed out ("the current node timed out during startup …
GCS has become overloaded"), and the workers reported "No node info found for head node in GCS." The
head's raylet / GCS processes were CPU-starved by the driver step's inherited CPU mask. Re-running the
driver step with **`--cpu-bind=none`** gave the nested Ray daemons the full node and the cluster came up
cleanly. The later matched payload ladder must launch the Ray driver step with `--cpu-bind=none`.

## Result (job 159228)

Fresh 3-node `medusa` allocation, `--exclusive`, subnet `10.42.5.x`, Ray 2.55.1. Head / coordinator =
medusa11 (`num_cpus=0`), workers = medusa12 / medusa13 (`worker-cpus=8`). Settings: N=8, prewarm=3,
measured=5, sizes {0, 262144}, n-remote=2, ray-port=6379, ray-dispatch-timeout=30 s. Per-size artifact
filenames encode job id + S + `ray` (no clobbering vs the HPX `_hpx.json` artifacts).

| S (bytes) | overall_pass | calls | dispatch timeout | orphans after teardown | RTT observation (Ray-only sanity, NOT comparison) |
| --- | --- | --- | --- | --- | --- |
| 0 | **True** | 5/5 | none | none | mean ≈ 4.14 ms |
| 262144 | **True** | 5/5 | none | none | mean ≈ 56.79 ms |

The RTT numbers are **within-arm Ray mechanism observations only** — they are not compared to the HPX
arm, no ratio is computed, and `same_axis_comparison` stays False.

### Gates (all True, both sizes, all 5 calls)

| gate | S=0 | S=262144 |
| --- | --- | --- |
| coordinator on head node | True | True |
| coordinator `num_cpus=0` | True | True |
| ray head `num_cpus=0` | True | True |
| coordinator runs zero leaves | True | True |
| hard NodeAffinity (`soft=False`) | True | True |
| leaves_local = 0 | True | True |
| leaves_remote = N (8) | True | True |
| witness_leaf_count = N (8) | True | True |
| leaves_per_remote_node = 4/4 | True | True |
| every remote node covered | True | True |
| scalar oracle correct | True | True |
| payload byte length correct | True | True |
| payload digest correct (after timing) | True | True |
| no dispatch timeout | True | True |
| no orphan Ray processes after teardown | True | True |
| fences locked False | True | True |
| `same_axis_comparison=false` | True | True |

## Interpretation

* Slice 2 establishes that the **Ray payload-fanin arm exists and passes every payload-smoke gate** at
  S=0 and S=262144: control-plane coordinator on the head node running zero leaves, N=8 leaves
  hard-pinned 4/4 across two remote workers (all leaves remote), correct scalar oracle, correct payload
  byte length, and correct post-timing payload digest, with no dispatch timeout and no orphaned Ray
  processes after teardown.
* The HPX Slice 1 smoke already passed at the **same** smoke sizes, so both arms now have green payload
  smokes at S=0 and S=262144 — but this report **does not compare their numbers**. It only records that
  the second arm is available and structurally gated for the later matched ladder.

## What this does and does not license (claim discipline)

* Experiment-only; not shipped `rayx.runtime`; not distributed RayX API; not an object store; not
  arbitrary Python execution beyond a fixed synthetic coordinator/leaf; not Ray Serve; not real
  inference (the payload is a deterministic synthetic sawtooth, not model output).
* This is a **mechanism smoke** for the Ray arm — it establishes no performance, no payload-size scaling
  law, no Ray-vs-HPX comparison, no same-axis evidence, and no production behavior.
* No ratios, no speedups, no winner language. `same_axis_comparison` remains False and is to be flipped
  True only by a later aggregate whose correlation gates all pass.

## Roadmap impact

* **Roadmap strengthened.** Both arms now exist and pass payload-smoke gates at S=0 and S=262144, so the
  matched payload ladder is unblocked.

## Next recommended step

* Design the **matched payload ladder** for `[0, 64, 1024, 16384, 262144]`:
  * both arms in one fresh allocation if feasible;
  * HPX phase first, then Ray phase;
  * the Ray driver step must use `--cpu-bind=none`;
  * the aggregate stays no-ratio / no-speedup / no-winner unless and until explicit structural gates are
    satisfied;
  * `same_axis_comparison` flips True only in a later aggregate if all correlation gates pass.

## Artifacts

Gitignored under `_exp64_runs/ray_payload_smoke_copyback_159228/` (copied back from Rostam; source not
synced back):

* `exp64_payload_smoke_159228_S0_ray.json`
* `exp64_payload_smoke_159228_S262144_ray.json`
* `ray_payload_159228.log`

## Reproduce

```
srun --jobid=<JOB> --overlap --cpu-bind=none -N1 -n1 --nodelist=<first-node> bash -lc \
  '... module load + venv activate ...; python -u run_exp64_payload.py \
     --phase ray-payload-remote-smoke --n 8 --smoke-sizes 0,262144 --prewarm 3 --measured 5 \
     --n-remote 2 --prefer-subnet 10.42.5. --ray-port 6379 --worker-cpus 8 --ray-dispatch-timeout-s 30'
```

Needs a ≥3-node Slurm allocation with Ray installed; the phase skips cleanly off-cluster or without Ray.
The driver runs as the root step on the first allocated compute node with the full allocation nodelist
restored in its environment and **`--cpu-bind=none`**; it bootstraps the Ray head + two workers via
`ray start --block` srun steps and connects with `ray.init`.
