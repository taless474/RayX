#!/bin/bash
# exp70 Slice 2A -- TWO-node Rostam cross-node launcher for the explicit-completion contract.
# The batch step runs ON the first allocated node (node A), co-located with the Slice 2A
# controller, the Ray head, the separately supervised WORK-FREE HPX root, and actor A. Actor B
# is hard-placed on node B. Modeled on the validated exp70_slice1_crossnode.sbatch and
# exp68_crossnode.sbatch; uses the VERIFIED fixed HPX install and the EXISTING exp68
# build_rostam artifacts. This launcher NEVER rebuilds or modifies exp68.
#
#   sbatch --wait experiments/70_hpx_locality_supervision/slice2_explicit_completion/exp70_slice2_crossnode.sbatch
#
#SBATCH -J exp70s2xn
#SBATCH -p medusa
#SBATCH -N 2
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH -t 01:30:00
#SBATCH -o /work/bitayekrang/RayX/experiments/70_hpx_locality_supervision/slice2_explicit_completion/_exp70_slice2_runs/exp70s2xn_%j.out

set -euo pipefail

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source /work/bitayekrang/venvs/rayx-a2b/bin/activate
export SLURM_EXPORT_ENV=ALL

REPO=/work/bitayekrang/RayX
S2=$REPO/experiments/70_hpx_locality_supervision/slice2_explicit_completion
EXP68=$REPO/experiments/68_vocab_sharded_topk
BUILD=$EXP68/build_rostam
HPX=/work/bitayekrang/apps/hpx-master-20bc3d4b-install
FIXED_SHA=20bc3d4bf3068383edcb63be13f22e9ff95842fa
SUBNET=10.42.5.
RUNS=$S2/_exp70_slice2_runs
export LD_LIBRARY_PATH="$HPX/lib:$HPX/lib64:${LD_LIBRARY_PATH:-}"

mkdir -p "$RUNS"
IDENT=$RUNS/hardware_identity_${SLURM_JOB_ID}.txt
exec_ident () { { echo "### $*"; "$@" 2>&1 || true; echo; } >> "$IDENT"; }

echo "=== identity gate: fixed HPX install SHA ==="
grep -q "$FIXED_SHA" "$HPX/include/hpx/config/defines.hpp" \
  || { echo "FATAL: fixed HPX SHA not found in $HPX"; exit 1; }
echo "ok: $HPX carries $FIXED_SHA"

echo "=== allocation ==="
echo "SLURM_JOB_ID=$SLURM_JOB_ID"
echo "SLURM_JOB_NODELIST=$SLURM_JOB_NODELIST"
mapfile -t NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
if [ "${#NODES[@]}" -ne 2 ]; then
  echo "FATAL: Slice 2A cross-node requires exactly TWO nodes; got ${#NODES[@]}: ${NODES[*]}"
  exit 1
fi
for nd in "${NODES[@]}"; do
  case "$nd" in medusa*) ;; *) echo "FATAL: non-medusa node in allocation: $nd"; exit 1 ;; esac
done
if [ "${NODES[0]}" = "${NODES[1]}" ]; then
  echo "FATAL: allocation is not two DISTINCT nodes: ${NODES[*]}"; exit 1
fi
# run_slice2.py resolves nodeA as the SORTED-first node and refuses to run the controller
# anywhere else; gate that here so a misplaced batch step fails loudly instead of skipping.
NODEA=$(printf '%s\n' "${NODES[@]}" | sort | head -1)
NODEB=$(printf '%s\n' "${NODES[@]}" | sort | tail -1)
HOSTFULL=$(hostname)
HOSTSHORT=${HOSTFULL%%.*}
if [ "$HOSTSHORT" != "${NODEA%%.*}" ]; then
  echo "FATAL: batch step on $HOSTSHORT but controller must run on nodeA=$NODEA"; exit 1
fi
echo "nodeA(controller/head/root/actorA)=$NODEA  nodeB(actorB)=$NODEB  host=$HOSTFULL"

echo "=== environment / HPX / Ray / source identity -> $IDENT ==="
{
  echo "created_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "slurm_job_id=$SLURM_JOB_ID"
  echo "slurm_job_nodelist=$SLURM_JOB_NODELIST"
  echo "slurm_partition=${SLURM_JOB_PARTITION:-}"
  echo "nodeA=$NODEA"
  echo "nodeB=$NODEB"
  echo "batch_host=$HOSTFULL"
  echo "subnet=$SUBNET"
  echo "hpx_install=$HPX"
  echo "hpx_fixed_sha=$FIXED_SHA"
  echo "venv=${VIRTUAL_ENV:-}"
  echo
} > "$IDENT"
exec_ident module list
exec_ident python -V
exec_ident python -c "import ray; print('ray', ray.__version__)"
exec_ident scontrol show hostnames "$SLURM_JOB_NODELIST"
exec_ident ip -o -4 addr show
exec_ident srun -N1 -n1 -w "$NODEB" ip -o -4 addr show
exec_ident sha256sum "$S2/run_slice2.py" "$S2/exp70_slice2_crossnode.sbatch" "$S2/.gitignore"
exec_ident sha256sum "$BUILD/exp68_peer"
for so in "$BUILD"/exp68_actor_ext*.so; do exec_ident sha256sum "$so"; done
exec_ident sha256sum "$EXP68/run_exp68.py" "$EXP68/exp68_peer.cpp" "$EXP68/actor_ext.cpp" \
  "$EXP68/shared_topk.hpp" "$EXP68/topk_action.hpp" "$EXP68/CMakeLists.txt"

echo "=== exp68 artifacts (must already exist; this launcher never rebuilds) ==="
test -x "$BUILD/exp68_peer" || { echo "FATAL: exp68_peer missing at $BUILD"; exit 1; }
ls "$BUILD"/exp68_actor_ext*.so >/dev/null \
  || { echo "FATAL: exp68 actor ext .so missing at $BUILD"; exit 1; }
echo "ok: using prebuilt $BUILD (no configure, no build)"

echo "=== syntax + selftest (inside allocation) ==="
python -m py_compile "$S2/run_slice2.py"
python "$S2/run_slice2.py" --selftest

# $1 = run label, $2 = HPX port base, $3 = Ray port. Each live invocation gets a DISJOINT port
# block (root/actorA/actorB take base, base+1, base+2) and its own Ray head port, so the two
# runs never share an endpoint even though they are strictly sequential.
run_once () {
  echo "=== cross-node run $1 start $(date -u +%Y-%m-%dT%H:%M:%SZ) (port-base=$2 ray-port=$3) ==="
  python "$S2/run_slice2.py" \
    --phase rostam-cross-node \
    --exp68-build-dir "$BUILD" \
    --subnet "$SUBNET" \
    --port-base "$2" \
    --ray-port "$3"
  local d
  d=$(ls -dt "$RUNS"/crossnode_"$SLURM_JOB_ID"_*/ | head -1)
  python - "$d/aggregate.json" <<'EOF'
import json, sys
a = json.load(open(sys.argv[1]))
print(f"[launcher] verdict: overall={a.get('overall')} class={a.get('failure_class')} "
      f"failed={sorted((a.get('gates_failed') or {}).keys())} runid={a.get('runid')}")
sys.exit(0 if a.get("overall") == "pass" else 1)
EOF
}

RUN1=fail; RUN2=skipped
if run_once 1 7911 6479; then
  RUN1=pass
  echo "=== first run PASS; starting second live run (fresh runid/ports/dirs/actors) ==="
  if run_once 2 7931 6499; then RUN2=pass; else RUN2=fail; echo "SECOND RUN DID NOT PASS (artifacts preserved)"; fi
else
  echo "FIRST RUN DID NOT PASS (artifacts preserved; no second run)"
fi
echo "run1=$RUN1 run2=$RUN2"

echo "=== experiment-scoped cleanup + orphan sweep ==="
SWEEP=$RUNS/orphan_sweep_${SLURM_JOB_ID}.txt
sweep_node () {
  echo "--- $1 ---"
  srun -N1 -n1 -w "$1" bash -lc \
    'ps -u "$USER" -o pid=,ppid=,etimes=,comm=,args= \
       | grep -E "exp68_peer|raylet|gcs_server|ray::|plasma_store" \
       | grep -v grep || echo "none"' 2>&1 || true
}
{ echo "post-run sweep $(date -u +%Y-%m-%dT%H:%M:%SZ) (experiment-scoped: exp68_peer + Ray)"
  sweep_node "$NODEA"; sweep_node "$NODEB"; } > "$SWEEP" 2>&1
cat "$SWEEP"
# Slice 2A's own owned-process sweep already ran per-run and is gated in aggregate.json; this
# is a coarse belt-and-braces cleanup for anything the allocation would otherwise leak.
# The bracketed class stops the pattern from matching the sweeping command's own argv
# (an unbracketed -f exp68_peer makes srun's remote shell kill itself and log a spurious
# "task 0: Terminated").
srun -N1 -n1 -w "$NODEB" bash -lc 'pkill -u "$USER" -f "[e]xp68_peer" || true' 2>&1 || true
pkill -u "$USER" -f "[e]xp68_peer" 2>/dev/null || true
ray stop --force >/dev/null 2>&1 || true
rm -rf /tmp/exp70s2a_ray_"$SLURM_JOB_ID"_* 2>/dev/null || true

echo "=== hardware evidence manifest + hashes ==="
sacct -j "$SLURM_JOB_ID" -P -o JobID,State,Start,End,Elapsed,NodeList > "$RUNS/sacct_${SLURM_JOB_ID}.txt" 2>&1 || true
python - "$RUNS" "$S2" "$EXP68" "$BUILD" "$SLURM_JOB_ID" "$IDENT" "$SWEEP" <<'PYEOF'
import hashlib, json, os, subprocess, sys, time

runs_dir, s2, exp68, build, job, ident, sweep = sys.argv[1:8]

def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()

def hashes(base, names):
    out = {}
    for n in names:
        p = os.path.join(base, n)
        if os.path.isfile(p):
            out[n] = sha256(p)
    return out

run_dirs = sorted(d for d in os.listdir(runs_dir)
                  if d.startswith(f"crossnode_{job}_") and os.path.isdir(os.path.join(runs_dir, d)))

artifacts, runs = {}, []
for d in run_dirs:
    full = os.path.join(runs_dir, d)
    for root, _, files in os.walk(full):
        for fn in files:
            p = os.path.join(root, fn)
            artifacts[os.path.relpath(p, runs_dir)] = sha256(p)
    agg_p = os.path.join(full, "aggregate.json")
    rec = {"runid": d.split("_")[-1], "runs_dir": full,
           "files": sorted(os.listdir(full))}
    if os.path.isfile(agg_p):
        a = json.load(open(agg_p))
        rec.update({"overall": a.get("overall"), "failure_class": a.get("failure_class"),
                    "gates_failed": sorted((a.get("gates_failed") or {}).keys()),
                    "work1_case": a.get("work1_case"), "work2_case": a.get("work2_case"),
                    "configured_idle_s": a.get("configured_idle_s"),
                    "epoch_id": a.get("epoch_id")})
    runs.append(rec)

for extra in (ident, sweep, os.path.join(runs_dir, f"sacct_{job}.txt"),
              os.path.join(runs_dir, f"exp70s2xn_{job}.out")):
    if os.path.isfile(extra):
        artifacts[os.path.relpath(extra, runs_dir)] = sha256(extra)

built = {os.path.basename(p): sha256(p) for p in
         [os.path.join(build, "exp68_peer")] +
         [os.path.join(build, f) for f in sorted(os.listdir(build))
          if f.startswith("exp68_actor_ext") and f.endswith(".so")]
         if os.path.isfile(p)}

man = {
    "experiment": "exp70_slice2a_explicit_completion",
    "phase": "rostam-cross-node",
    "manifest_scope": "hardware-evidence inventory; no result analysis performed on Rostam",
    "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "slurm": {"job_id": job, "nodelist": os.environ.get("SLURM_JOB_NODELIST", ""),
              "partition": os.environ.get("SLURM_JOB_PARTITION", ""),
              "sacct": open(os.path.join(runs_dir, f"sacct_{job}.txt")).read().splitlines()
              if os.path.isfile(os.path.join(runs_dir, f"sacct_{job}.txt")) else []},
    "environment": {"hpx_install": os.environ.get("HPX_INSTALL_RECORDED",
                                                  "/work/bitayekrang/apps/hpx-master-20bc3d4b-install"),
                    "hpx_fixed_sha": "20bc3d4bf3068383edcb63be13f22e9ff95842fa",
                    "modules": "gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 "
                               "python/3.12.3 (module purge first)",
                    "venv": os.environ.get("VIRTUAL_ENV", ""),
                    "subnet": "10.42.5.",
                    "identity_file": os.path.basename(ident)},
    "commands": [
        "sbatch --wait experiments/70_hpx_locality_supervision/slice2_explicit_completion/"
        "exp70_slice2_crossnode.sbatch",
        "python -m py_compile run_slice2.py",
        "python run_slice2.py --selftest  (inside allocation)",
        "python run_slice2.py --phase rostam-cross-node --exp68-build-dir <exp68>/build_rostam "
        "--subnet 10.42.5.  (x2, fresh runid/ports/dirs/actors each)",
    ],
    "source_hashes": hashes(s2, ["run_slice2.py", "exp70_slice2_crossnode.sbatch",
                                 ".gitignore", "README.md"]),
    "exp68_dependency_hashes": hashes(exp68, ["run_exp68.py", "exp68_peer.cpp", "actor_ext.cpp",
                                              "shared_topk.hpp", "topk_action.hpp",
                                              "CMakeLists.txt"]),
    "exp68_built_artifacts": built,
    "evidence_pointers": {
        "placement": "per-run aggregate.json gates.startup + markers.json crossnode section "
                     "(ray_node_ids, hard NodeAffinitySchedulingStrategy soft=False)",
        "in_process_hosting": "per-run aggregate.json gates.inprocess (pid identity, no HPX children)",
        "workloads": "per-run aggregate.json gates.work1/gates.work2 (bit-exact vs exp68 oracle, "
                     "both coordinator directions)",
        "idle": "per-run aggregate.json gates.idle + markers.json phase_times_wall_ms",
        "completion": "per-run aggregate.json gates.completion/epoch_scope/observation",
        "fence": "per-run aggregate.json gates.post_completion_fence "
                 "(post_completion_dispatch_never_reached_hpx)",
        "departure_finalize": "per-run aggregate.json gates.departure/root_alone/finalize",
        "cleanup": "per-run aggregate.json gates.final + orphan_sweep file",
        "ordering": "per-run aggregate.json gates.ordering + markers.json state_history",
    },
    "runs": runs,
    "artifact_sha256": dict(sorted(artifacts.items())),
}
out = os.path.join(runs_dir, f"hardware_evidence_manifest_{job}.json")
with open(out, "w") as f:
    json.dump(man, f, indent=2, sort_keys=True)
with open(out + ".sha256", "w") as f:
    f.write(f"{sha256(out)}  {os.path.basename(out)}\n")
print(f"[manifest] {out}  runs={len(runs)} artifacts={len(artifacts)}")
for r in runs:
    print(f"[manifest] run {r['runid']}: overall={r.get('overall')} "
          f"class={r.get('failure_class')} failed={r.get('gates_failed')}")
PYEOF

echo "=== done: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
[ "$RUN1" = pass ] && [ "$RUN2" = pass ]
