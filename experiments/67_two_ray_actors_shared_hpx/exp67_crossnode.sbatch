#!/bin/bash
# exp67 -- THREE-node Rostam cross-node slice. The batch step runs ON the first allocated node
# (node R), co-located with the Ray head and the work-free HPX root, so run_exp67.py attaches to
# Ray directly (no inner/outer srun split). Ray actor A is hard-placed on node A, actor B on node B;
# each hosts an HPX connect-mode locality IN-PROCESS, and the load-bearing proof is bidirectional
# cross-node actor-to-actor HPX (A<->B). Builds the EXPERIMENT-ONLY targets against the VERIFIED
# fixed HPX install. EXPERIMENT-ONLY: no production _rayx, no perf claim.
#
#   sbatch -w medusa00,medusa01,medusa02 \
#     experiments/67_two_ray_actors_shared_hpx/exp67_crossnode.sbatch
#
#SBATCH -J exp67xn
#SBATCH -p medusa
#SBATCH -N 3
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH -t 01:00:00
#SBATCH -o /work/bitayekrang/RayX/experiments/67_two_ray_actors_shared_hpx/_exp67_runs/exp67xn_%j.out

set -euo pipefail

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source /work/bitayekrang/venvs/rayx-a2b/bin/activate
export SLURM_EXPORT_ENV=ALL

REPO=/work/bitayekrang/RayX
EXP=$REPO/experiments/67_two_ray_actors_shared_hpx
HPX=/work/bitayekrang/apps/hpx-master-20bc3d4b-install
FIXED_SHA=20bc3d4bf3068383edcb63be13f22e9ff95842fa

# HPX runtime libs must be resolvable on ALL THREE nodes (the actors' .so + the peer link libhpx).
export LD_LIBRARY_PATH="$HPX/lib:$HPX/lib64:${LD_LIBRARY_PATH:-}"

echo "=== identity gate: fixed HPX install SHA ==="
grep -q "$FIXED_SHA" "$HPX/include/hpx/config/defines.hpp" \
  || { echo "FATAL: fixed HPX SHA not found in $HPX"; exit 1; }
echo "ok: $HPX carries $FIXED_SHA"

echo "=== allocation ==="
echo "SLURM_JOB_ID=$SLURM_JOB_ID"
echo "SLURM_JOB_NODELIST=$SLURM_JOB_NODELIST"
mapfile -t NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
if [ "${#NODES[@]}" -lt 3 ]; then
  echo "FATAL: exp67 cross-node requires a THREE-node allocation; got ${#NODES[@]}: ${NODES[*]}"
  echo "Do NOT collapse actor A and actor B onto one node. Two-node fallback is out of scope."
  exit 1
fi
NODER="${NODES[0]}"; NODEA="${NODES[1]}"; NODEB="${NODES[2]}"
echo "nodeR(root/controller)=$NODER  actorA=$NODEA  actorB=$NODEB  host=$(hostname)"

echo "=== build exp67 (experiment-only) against fixed HPX ==="
PYBIND_CMK="$(python -m pybind11 --cmakedir)"
cmake -S "$EXP" -B "$EXP/build_rostam" -G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
  -DPython_EXECUTABLE="$(which python)" \
  -DCMAKE_PREFIX_PATH="$HPX;$PYBIND_CMK"
cmake --build "$EXP/build_rostam" -j 8

REPS="${1:-3}"   # $1: pass 1 for a smoke rep; default is the full 3-rep set
AGG="${2:-}"     # $2 (optional): aggregate path; smoke should use a scratch path under _exp67_runs
AGG_ARG=()
[ -n "$AGG" ] && AGG_ARG=(--aggregate "$AGG")
echo "=== run ${REPS}-rep three-node cross-node experiment (controller on $NODER) ==="
python "$EXP/run_exp67.py" --phase rostam-cross-node --reps "$REPS" \
  --build-dir "$EXP/build_rostam" \
  --root-node "$NODER" --actor-a-node "$NODEA" --actor-b-node "$NODEB" \
  --subnet-prefix 10.42.5. "${AGG_ARG[@]}"

echo "=== done: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
