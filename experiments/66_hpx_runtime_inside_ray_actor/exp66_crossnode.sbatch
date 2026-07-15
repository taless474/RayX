#!/bin/bash
# exp66 -- two-node Rostam cross-node slice. Batch step runs ON nodeA (first allocated node),
# co-located with the Ray head, so run_exp66.py attaches to Ray directly (no inner/outer srun
# split). Builds the EXPERIMENT-ONLY targets against the VERIFIED fixed HPX install, then drives
# the 3-rep cross-node experiment. EXPERIMENT-ONLY: no production _rayx, no perf claim.
#
#   sbatch -w medusa00,medusa01 experiments/66_hpx_runtime_inside_ray_actor/exp66_crossnode.sbatch
#
#SBATCH -J exp66xn
#SBATCH -p medusa
#SBATCH -N 2
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH -t 00:45:00
#SBATCH -o /work/bitayekrang/RayX/experiments/66_hpx_runtime_inside_ray_actor/_exp66_runs/exp66xn_%j.out

set -euo pipefail

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source /work/bitayekrang/venvs/rayx-a2b/bin/activate
export SLURM_EXPORT_ENV=ALL

REPO=/work/bitayekrang/RayX
EXP=$REPO/experiments/66_hpx_runtime_inside_ray_actor
HPX=/work/bitayekrang/apps/hpx-master-20bc3d4b-install
FIXED_SHA=20bc3d4bf3068383edcb63be13f22e9ff95842fa

# HPX runtime libs must be resolvable on BOTH nodes (the actor's .so + the peer link libhpx).
export LD_LIBRARY_PATH="$HPX/lib:$HPX/lib64:${LD_LIBRARY_PATH:-}"

echo "=== identity gate: fixed HPX install SHA ==="
grep -q "$FIXED_SHA" "$HPX/include/hpx/config/defines.hpp" \
  || { echo "FATAL: fixed HPX SHA not found in $HPX"; exit 1; }
echo "ok: $HPX carries $FIXED_SHA"

echo "=== allocation ==="
echo "SLURM_JOB_ID=$SLURM_JOB_ID"
echo "SLURM_JOB_NODELIST=$SLURM_JOB_NODELIST"
mapfile -t NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
NODEA="${NODES[0]}"; NODEB="${NODES[1]}"
echo "nodeA(root/controller)=$NODEA  nodeB(actor)=$NODEB  host=$(hostname)"

echo "=== build exp66 (experiment-only) against fixed HPX ==="
PYBIND_CMK="$(python -m pybind11 --cmakedir)"
cmake -S "$EXP" -B "$EXP/build_rostam" -G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
  -DPython_EXECUTABLE="$(which python)" \
  -DCMAKE_PREFIX_PATH="$HPX;$PYBIND_CMK"
cmake --build "$EXP/build_rostam" -j 8

echo "=== run 3-rep cross-node experiment (controller on $NODEA) ==="
python "$EXP/run_exp66.py" --phase rostam-cross-node --reps 3 \
  --build-dir "$EXP/build_rostam" \
  --root-node "$NODEA" --actor-node "$NODEB" \
  --subnet-prefix 10.42.5.

echo "=== done: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
