#!/bin/bash
# exp70 Slice 1 -- TWO-node Rostam cross-node launcher. The batch step runs ON the first
# allocated node (node A), co-located with the Slice 1 controller, the Ray head, the
# separately supervised work-free HPX root, and actor A. Actor B (the failure-injection
# victim) is hard-placed on node B. Modeled on the validated exp68_crossnode.sbatch; uses
# the VERIFIED fixed HPX install and the EXISTING exp68 build_rostam artifacts (no rebuild).
#
#   sbatch experiments/70_hpx_locality_supervision/slice1_actor_hosted_island_restart/exp70_slice1_crossnode.sbatch
#
#SBATCH -J exp70s1xn
#SBATCH -p medusa
#SBATCH -N 2
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH -t 01:30:00
#SBATCH -o /work/bitayekrang/RayX/experiments/70_hpx_locality_supervision/slice1_actor_hosted_island_restart/_exp70_slice1_runs/exp70s1xn_%j.out

set -euo pipefail

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source /work/bitayekrang/venvs/rayx-a2b/bin/activate
export SLURM_EXPORT_ENV=ALL

REPO=/work/bitayekrang/RayX
S1=$REPO/experiments/70_hpx_locality_supervision/slice1_actor_hosted_island_restart
EXP68=$REPO/experiments/68_vocab_sharded_topk
HPX=/work/bitayekrang/apps/hpx-master-20bc3d4b-install
FIXED_SHA=20bc3d4bf3068383edcb63be13f22e9ff95842fa
export LD_LIBRARY_PATH="$HPX/lib:$HPX/lib64:${LD_LIBRARY_PATH:-}"

echo "=== identity gate: fixed HPX install SHA ==="
grep -q "$FIXED_SHA" "$HPX/include/hpx/config/defines.hpp" \
  || { echo "FATAL: fixed HPX SHA not found in $HPX"; exit 1; }
echo "ok: $HPX carries $FIXED_SHA"

echo "=== allocation ==="
echo "SLURM_JOB_ID=$SLURM_JOB_ID"
echo "SLURM_JOB_NODELIST=$SLURM_JOB_NODELIST"
mapfile -t NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
if [ "${#NODES[@]}" -ne 2 ]; then
  echo "FATAL: Slice 1 cross-node requires exactly TWO nodes; got ${#NODES[@]}: ${NODES[*]}"
  exit 1
fi
for nd in "${NODES[@]}"; do
  case "$nd" in medusa*) ;; *) echo "FATAL: non-medusa node in allocation: $nd"; exit 1 ;; esac
done
echo "nodeA(controller/head/root/actorA)=${NODES[0]}  nodeB(actorB=victim)=${NODES[1]}  host=$(hostname)"

echo "=== environment identity ==="
date -u +%Y-%m-%dT%H:%M:%SZ
module list 2>&1
python -V
python -c "import ray; print('ray', ray.__version__)"
ip -o -4 addr show 2>/dev/null | grep '10\.42\.' || true
sha256sum "$S1/run_slice1.py" "$EXP68/build_rostam/exp68_peer" "$EXP68"/build_rostam/exp68_actor_ext*.so

echo "=== exp68 artifacts (must already exist; this launcher never rebuilds) ==="
test -x "$EXP68/build_rostam/exp68_peer" || { echo "FATAL: exp68_peer missing"; exit 1; }
ls "$EXP68"/build_rostam/exp68_actor_ext*.so >/dev/null \
  || { echo "FATAL: exp68 actor ext .so missing"; exit 1; }

echo "=== syntax + selftest (inside allocation) ==="
python -m py_compile "$S1/run_slice1.py"
python "$S1/run_slice1.py" --selftest

run_once () {
  echo "=== cross-node run $1 start $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  python "$S1/run_slice1.py" --phase rostam-cross-node --exp68-build-dir "$EXP68/build_rostam"
  local d
  d=$(ls -dt "$S1/_exp70_slice1_runs"/crossnode_"$SLURM_JOB_ID"_*/ | head -1)
  python - "$d/aggregate.json" <<'EOF'
import json, sys
a = json.load(open(sys.argv[1]))
print(f"[launcher] verdict: overall={a.get('overall')} class={a.get('failure_class')} "
      f"post_loss={a.get('post_loss_membership_category')} runid={a.get('runid')}")
sys.exit(0 if a.get("overall") == "pass" else 1)
EOF
}

if run_once 1; then
  echo "=== first run PASS; starting repeatability run ==="
  run_once 2 || echo "REPEAT RUN DID NOT PASS (artifacts preserved)"
else
  echo "FIRST RUN DID NOT PASS (artifacts preserved; no repeat run)"
fi
echo "=== done: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
