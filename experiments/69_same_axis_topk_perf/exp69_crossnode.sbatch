#!/bin/bash
# exp69 -- THREE-node Rostam cross-node harness for the SAME-AXIS top-k performance probe. The batch
# step runs ON the first allocated node (node R), co-located with the Ray head and the WORK-FREE HPX
# root (locality 0). Ray actor A is HARD-placed (NodeAffinitySchedulingStrategy soft=False) on node A,
# actor B on node B; each hosts an HPX connect-mode locality in-process over the real TCP parcelport
# on the requested 10.42.5.x subnet. The SAME Python caller boundary measures a Ray-mediated and an
# HPX-mediated peer-candidate path (per-arm QD1 latency distributions ONLY -- no ratio/speedup/winner,
# all four ratio fences stay false). Builds the EXPERIMENT-ONLY targets against the VERIFIED fixed HPX.
#
# Run one STAGE per submission (so the smoke can be INSPECTED before the accepted run, and so the
# accepted aggregate contains 3 reps from ONE uninterrupted job):
#   # medusa02 may be occupied; any distinct third medusa node is acceptable (recorded in aggregate):
#   # Slice 1 (QD1 latency):
#   sbatch -w medusa00,medusa01,medusa02 experiments/69_same_axis_topk_perf/exp69_crossnode.sbatch diag
#   sbatch -w medusa00,medusa01,medusa02 experiments/69_same_axis_topk_perf/exp69_crossnode.sbatch smoke
#   sbatch -w medusa00,medusa01,medusa02 experiments/69_same_axis_topk_perf/exp69_crossnode.sbatch accepted
#   # Slice 2 (bounded-concurrency throughput; A-coordinator only, cases P0/P2/P3b, C in {2,4}):
#   sbatch -w medusa00,medusa01,medusa02 experiments/69_same_axis_topk_perf/exp69_crossnode.sbatch tput-smoke
#   sbatch -w medusa00,medusa01,medusa02 experiments/69_same_axis_topk_perf/exp69_crossnode.sbatch tput-accepted
#   # Slice 3 (DIAGNOSTIC causal matrix; P3b only, 3 resource bands C2/cpu2/ht2, C4/cpu2/ht2, C4/cpu4/ht4):
#   sbatch -w medusa00,medusa01,medusa02 experiments/69_same_axis_topk_perf/exp69_crossnode.sbatch slice3-smoke
#   sbatch -w medusa00,medusa01,medusa02 experiments/69_same_axis_topk_perf/exp69_crossnode.sbatch slice3-accepted
#
#SBATCH -J exp69xn
#SBATCH -p medusa
#SBATCH -N 3
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH -t 01:30:00
#SBATCH -o /work/bitayekrang/RayX/experiments/69_same_axis_topk_perf/_exp69_runs/exp69xn_%j.out

set -euo pipefail

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source /work/bitayekrang/venvs/rayx-a2b/bin/activate
export SLURM_EXPORT_ENV=ALL

REPO=/work/bitayekrang/RayX
EXP=$REPO/experiments/69_same_axis_topk_perf
HPX=/work/bitayekrang/apps/hpx-master-20bc3d4b-install
FIXED_SHA=20bc3d4bf3068383edcb63be13f22e9ff95842fa

export LD_LIBRARY_PATH="$HPX/lib:$HPX/lib64:${LD_LIBRARY_PATH:-}"

STAGE="${1:-smoke}"   # $1: diag | smoke | accepted | tput-smoke | tput-accepted | slice3-smoke | slice3-accepted  (run ONE per submission)

echo "=== identity gate: fixed HPX install SHA ==="
grep -q "$FIXED_SHA" "$HPX/include/hpx/config/defines.hpp" \
  || { echo "FATAL: fixed HPX SHA not found in $HPX"; exit 1; }
echo "ok: $HPX carries $FIXED_SHA"

echo "=== allocation ==="
echo "SLURM_JOB_ID=$SLURM_JOB_ID"
echo "SLURM_JOB_NODELIST=$SLURM_JOB_NODELIST"
mapfile -t NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
if [ "${#NODES[@]}" -lt 3 ]; then
  echo "FATAL: exp69 cross-node requires a THREE-node allocation; got ${#NODES[@]}: ${NODES[*]}"
  exit 1
fi
NODER="${NODES[0]}"; NODEA="${NODES[1]}"; NODEB="${NODES[2]}"
echo "nodeR(root/controller)=$NODER  actorA=$NODEA  actorB=$NODEB  host=$(hostname)  stage=$STAGE"

echo "=== build exp69 (experiment-only) against fixed HPX ==="
PYBIND_CMK="$(python -m pybind11 --cmakedir)"
cmake -S "$EXP" -B "$EXP/build_rostam" -G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
  -DPython_EXECUTABLE="$(which python)" \
  -DCMAKE_PREFIX_PATH="$HPX;$PYBIND_CMK"
cmake --build "$EXP/build_rostam" -j 8

BUILD="$EXP/build_rostam"
case "$STAGE" in
  diag)
    # SEPARATE, non-gating Linux timed-wait diagnostic (old wait_for vs hardened post+get). Runs on
    # nodeR only; independent of the measured Ray actors. Never contributes a performance sample.
    ITERS="${2:-800}"
    echo "=== Linux timed-wait diagnostic ($ITERS iters/mode; NON-GATING) ==="
    python "$EXP/timed_wait_diag.py" --build-dir "$BUILD" --iters "$ITERS" \
      --out "$EXP/_exp69_runs/timed_wait_diag_${SLURM_JOB_ID}.json"
    ;;
  smoke)
    # Slice 0: A-coordinator, all six timed cases, W=5 K=30 R=1. Scratch aggregate (NEVER curated).
    echo "=== Slice 0 cross-node SMOKE (controller on $NODER) ==="
    python "$EXP/run_exp69.py" --phase rostam-cross-node --sampling smoke --reps 1 \
      --build-dir "$BUILD" \
      --root-node "$NODER" --actor-a-node "$NODEA" --actor-b-node "$NODEB" \
      --subnet-prefix 10.42.5. \
      --aggregate "$EXP/_exp69_runs/smoke_crossnode_${SLURM_JOB_ID}.json"
    ;;
  accepted)
    # Slice 1: A-primary (six cases, W=50 K=500) + B-control (P0/P2/P3b, W=20 K=200), R=3, ONE job.
    # No --aggregate override => the curated cross-node aggregate is written on PASS.
    echo "=== Slice 1 cross-node ACCEPTED R=3 (controller on $NODER) ==="
    python "$EXP/run_exp69.py" --phase rostam-cross-node --sampling accepted --reps 3 \
      --build-dir "$BUILD" \
      --root-node "$NODER" --actor-a-node "$NODEA" --actor-b-node "$NODEB" \
      --subnet-prefix 10.42.5.
    ;;
  tput-smoke)
    # Slice 2 SMOKE: bounded-concurrency throughput instrument. A-coordinator only, cases P0/P2/P3b,
    # C in {2,4}, warm-up 10 / measured N=50 / R=1 (THROUGHPUT_SAMPLING["smoke"]). Per-C concurrency
    # soak runs first inside the phase. Scratch aggregate under _exp69_runs/ (gitignored; NEVER curated;
    # distinct from the QD1 aggregates). Same three-node topology, hard placement, and bring-up as Slice 1.
    echo "=== Slice 2 cross-node THROUGHPUT SMOKE (controller on $NODER, C=2,4) ==="
    python "$EXP/run_exp69.py" --phase rostam-cross-node-throughput --sampling smoke \
      --concurrency 2,4 \
      --build-dir "$BUILD" \
      --root-node "$NODER" --actor-a-node "$NODEA" --actor-b-node "$NODEB" \
      --subnet-prefix 10.42.5. \
      --aggregate "$EXP/_exp69_runs/tput_smoke_crossnode_${SLURM_JOB_ID}.json"
    ;;
  tput-accepted)
    # Slice 2 ACCEPTED: A-coordinator only, cases P0/P2/P3b, C in {2,4}, warm-up 100 / measured N=1000 /
    # R=3 (THROUGHPUT_SAMPLING["accepted"]) in ONE uninterrupted job. No --aggregate override => the
    # runner writes the curated throughput aggregate (same_axis_topk_throughput_crossnode_aggregate.json)
    # ONLY if the complete accepted run passes. Same three-node topology and gates as Slice 1.
    echo "=== Slice 2 cross-node THROUGHPUT ACCEPTED R=3 (controller on $NODER, C=2,4) ==="
    python "$EXP/run_exp69.py" --phase rostam-cross-node-throughput --sampling accepted \
      --concurrency 2,4 \
      --build-dir "$BUILD" \
      --root-node "$NODER" --actor-a-node "$NODEA" --actor-b-node "$NODEB" \
      --subnet-prefix 10.42.5.
    ;;
  slice3-smoke)
    # Slice 3 SMOKE: DIAGNOSTIC causal-decomposition instrument. P3b only, all THREE resource bands
    # (C2/cpu2/ht2, C4/cpu2/ht2, C4/cpu4/ht4), warm-up 20 / measured N=100 / R=1 (SLICE3_SAMPLING["smoke"]).
    # Each band is a fresh hard-placed island under its OWN (C, num_cpus, hpx_threads); the two C=4 bands
    # are independent and share no capability proof. Scratch aggregate under _exp69_runs/ (gitignored;
    # NEVER curated). Same three-node topology and bring-up as Slice 1/2; drives the timed diagnostic arms.
    echo "=== Slice 3 cross-node DIAGNOSTIC SMOKE (controller on $NODER, P3b x 3 bands) ==="
    python "$EXP/run_exp69.py" --phase rostam-cross-node-slice3 --slice3-mode smoke \
      --build-dir "$BUILD" \
      --root-node "$NODER" --actor-a-node "$NODEA" --actor-b-node "$NODEB" \
      --subnet-prefix 10.42.5. \
      --aggregate "$EXP/_exp69_runs/slice3_smoke_crossnode_${SLURM_JOB_ID}.json"
    ;;
  slice3-accepted)
    # Slice 3 ACCEPTED: the DECISIVE causal matrix. P3b only, all THREE resource bands, warm-up 100 /
    # measured N=1000 / R=3 (SLICE3_SAMPLING["accepted"]) in ONE uninterrupted job -> 3 bands x R=3 = 9
    # band repetitions. No --aggregate override => the runner writes the curated Slice 3 aggregate
    # (same_axis_topk_slice3_crossnode_aggregate.json) ONLY if the WHOLE matrix passes every gate. Per-arm
    # decomposition ONLY; all four ratio fences stay false; no ratio/speedup/difference/winner is computed.
    echo "=== Slice 3 cross-node DIAGNOSTIC ACCEPTED R=3 (controller on $NODER, P3b x 3 bands) ==="
    python "$EXP/run_exp69.py" --phase rostam-cross-node-slice3 --slice3-mode accepted \
      --build-dir "$BUILD" \
      --root-node "$NODER" --actor-a-node "$NODEA" --actor-b-node "$NODEB" \
      --subnet-prefix 10.42.5.
    ;;
  *)
    echo "FATAL: unknown stage '$STAGE' (use: diag | smoke | accepted | tput-smoke | tput-accepted | slice3-smoke | slice3-accepted)"; exit 2
    ;;
esac

echo "=== done: stage=$STAGE $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
