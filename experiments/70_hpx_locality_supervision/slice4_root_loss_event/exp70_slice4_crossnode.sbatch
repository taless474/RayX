#!/bin/bash
# exp70 Slice 4A -- TWO-node Rostam cross-node launcher for explicit ROOT completion vs
# unexpected ROOT loss. The batch step runs ON the first allocated node (node A), co-located
# with the Slice 4A controller, the Ray head, the separately supervised WORK-FREE HPX root
# (the process whose completion/loss this slice classifies), and actor A. Actor B is
# hard-placed on node B. Modeled on the validated exp70_slice3_crossnode.sbatch /
# exp70_slice2_crossnode.sbatch; uses the VERIFIED fixed HPX install and the EXISTING exp68
# build_rostam artifacts. This launcher NEVER rebuilds or modifies exp68.
#
#   sbatch --wait experiments/70_hpx_locality_supervision/slice4_root_loss_event/exp70_slice4_crossnode.sbatch
#
# ARTIFACT HASHING PROTOCOL (Slice 4 correction): the in-job manifest hashes only artifacts
# that are CLOSED AND STABLE. The Slurm stdout file is still being written by this very job, so
# it is listed as `post_job_hash_required` and is NOT part of the in-job hash set. A second
# manifest is generated on the Mac after `sbatch --wait` returns, covering the completed log.
# Final copy-back verification must therefore be exact: 0 missing, 0 mismatched.
#
#SBATCH -J exp70s4xn
#SBATCH -p medusa
#SBATCH -N 2
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH -t 01:30:00
#SBATCH -o /work/bitayekrang/RayX/experiments/70_hpx_locality_supervision/slice4_root_loss_event/_exp70_slice4_runs/exp70s4xn_%j.out

set -euo pipefail

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source /work/bitayekrang/venvs/rayx-a2b/bin/activate
export SLURM_EXPORT_ENV=ALL

REPO=/work/bitayekrang/RayX
S4=$REPO/experiments/70_hpx_locality_supervision/slice4_root_loss_event
EXP68=$REPO/experiments/68_vocab_sharded_topk
BUILD=$EXP68/build_rostam
HPX=/work/bitayekrang/apps/hpx-master-20bc3d4b-install
FIXED_SHA=20bc3d4bf3068383edcb63be13f22e9ff95842fa
SUBNET=10.42.5.
RUNS=$S4/_exp70_slice4_runs
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
  echo "FATAL: Slice 4A cross-node requires exactly TWO nodes; got ${#NODES[@]}: ${NODES[*]}"
  exit 1
fi
for nd in "${NODES[@]}"; do
  case "$nd" in medusa*) ;; *) echo "FATAL: non-medusa node in allocation: $nd"; exit 1 ;; esac
done
if [ "${NODES[0]}" = "${NODES[1]}" ]; then
  echo "FATAL: allocation is not two DISTINCT nodes: ${NODES[*]}"; exit 1
fi
# run_slice4.py resolves nodeA as the SORTED-first node and refuses to run the controller
# anywhere else; gate that here so a misplaced batch step fails loudly instead of skipping.
NODEA=$(printf '%s\n' "${NODES[@]}" | sort | head -1)
NODEB=$(printf '%s\n' "${NODES[@]}" | sort | tail -1)
HOSTFULL=$(hostname)
HOSTSHORT=${HOSTFULL%%.*}
if [ "$HOSTSHORT" != "${NODEA%%.*}" ]; then
  echo "FATAL: batch step on $HOSTSHORT but controller must run on nodeA=$NODEA"; exit 1
fi
echo "nodeA(controller/head/work-free root/actorA)=$NODEA  nodeB(actorB)=$NODEB  host=$HOSTFULL"

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
exec_ident sha256sum "$S4/run_slice4.py" "$S4/exp70_slice4_crossnode.sbatch" "$S4/.gitignore"
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
python -m py_compile "$S4/run_slice4.py"
python "$S4/run_slice4.py" --selftest

# $1 = run label, $2 = HPX port base, $3 = Ray port. Each live invocation gets a DISJOINT port
# block; within a run the normal and loss arms are separated further by --arm-port-stride, so
# the two fresh islands never share an endpoint.
run_once () {
  echo "=== cross-node run $1 start $(date -u +%Y-%m-%dT%H:%M:%SZ) (port-base=$2 ray-port=$3) ==="
  python "$S4/run_slice4.py" \
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
      f"normal={a.get('normal_event')} loss={a.get('loss_event')} "
      f"silence={a.get('loss_observed_silence_s')} bound={a.get('suspicion_bound_s')} "
      f"failed={sorted((a.get('gates_failed') or {}).keys())} runid={a.get('runid')}")
sys.exit(0 if a.get("overall") == "pass" else 1)
EOF
}

RUN1=fail; RUN2=skipped
if run_once 1 8011 6529; then
  RUN1=pass
  echo "=== first run PASS; starting second live run (fresh runid/ports/dirs/actors/root) ==="
  if run_once 2 8041 6549; then RUN2=pass; else RUN2=fail; echo "SECOND RUN DID NOT PASS (artifacts preserved)"; fi
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
# The bracketed class stops the pattern from matching the sweeping command's own argv.
srun -N1 -n1 -w "$NODEB" bash -lc 'pkill -u "$USER" -f "[e]xp68_peer" || true' 2>&1 || true
pkill -u "$USER" -f "[e]xp68_peer" 2>/dev/null || true
ray stop --force >/dev/null 2>&1 || true
rm -rf /tmp/exp70s4a_ray_"$SLURM_JOB_ID"_* 2>/dev/null || true

echo "=== in-job manifest (CLOSED artifacts only; Slurm log deferred) ==="
sacct -j "$SLURM_JOB_ID" -P -o JobID,State,Start,End,Elapsed,NodeList > "$RUNS/sacct_${SLURM_JOB_ID}.txt" 2>&1 || true
python - "$RUNS" "$S4" "$EXP68" "$BUILD" "$SLURM_JOB_ID" "$IDENT" "$SWEEP" <<'PYEOF'
import hashlib, json, os, sys, time

runs_dir, s4, exp68, build, job, ident, sweep = sys.argv[1:8]

def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()

def hashes(base, names):
    return {n: sha256(os.path.join(base, n)) for n in names
            if os.path.isfile(os.path.join(base, n))}

# The job's own stdout file is STILL BEING WRITTEN by this process. It is deliberately excluded
# from the hash set and declared for post-job hashing instead.
SLURM_LOG = f"exp70s4xn_{job}.out"

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
    rec = {"runid": d.split("_")[-1], "runs_dir": full, "files": sorted(os.listdir(full))}
    if os.path.isfile(agg_p):
        a = json.load(open(agg_p))
        rec.update({"overall": a.get("overall"), "failure_class": a.get("failure_class"),
                    "gates_failed": sorted((a.get("gates_failed") or {}).keys()),
                    "normal_event": a.get("normal_event"), "loss_event": a.get("loss_event"),
                    "suspicion_bound_s": a.get("suspicion_bound_s"),
                    "expected_refresh_s": a.get("expected_refresh_s"),
                    "loss_observed_silence_s": a.get("loss_observed_silence_s"),
                    "loss_classification_elapsed_s": a.get("loss_classification_elapsed_s"),
                    "loss_actor_observation_categories":
                        a.get("loss_actor_observation_categories"),
                    "classifier_fingerprint": a.get("classifier_fingerprint"),
                    "classifier_allowlist": a.get("classifier_allowlist"),
                    "epoch_ids": a.get("epoch_ids")})
    runs.append(rec)

for extra in (ident, sweep, os.path.join(runs_dir, f"sacct_{job}.txt")):
    if os.path.isfile(extra):
        artifacts[os.path.relpath(extra, runs_dir)] = sha256(extra)

built = {os.path.basename(p): sha256(p) for p in
         [os.path.join(build, "exp68_peer")] +
         [os.path.join(build, f) for f in sorted(os.listdir(build))
          if f.startswith("exp68_actor_ext") and f.endswith(".so")]
         if os.path.isfile(p)}

man = {
    "experiment": "exp70_slice4a_root_loss_event",
    "phase": "rostam-cross-node",
    "manifest_scope": ("hardware-evidence inventory over CLOSED artifacts only; no result "
                       "analysis performed on Rostam"),
    "hashing_protocol": ("in-job manifest covers only files that are closed and stable. The "
                         "Slurm stdout file is still being appended to by this job and is "
                         "listed under post_job_hash_required; it is hashed on the analysis "
                         "host after `sbatch --wait` returns. Final copy-back verification "
                         "must be exact for every artifact."),
    "post_job_hash_required": [SLURM_LOG],
    "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "slurm": {"job_id": job, "nodelist": os.environ.get("SLURM_JOB_NODELIST", ""),
              "partition": os.environ.get("SLURM_JOB_PARTITION", ""),
              "sacct": open(os.path.join(runs_dir, f"sacct_{job}.txt")).read().splitlines()
              if os.path.isfile(os.path.join(runs_dir, f"sacct_{job}.txt")) else []},
    "environment": {"hpx_install": "/work/bitayekrang/apps/hpx-master-20bc3d4b-install",
                    "hpx_fixed_sha": "20bc3d4bf3068383edcb63be13f22e9ff95842fa",
                    "modules": "gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 "
                               "python/3.12.3 (module purge first)",
                    "venv": os.environ.get("VIRTUAL_ENV", ""),
                    "subnet": "10.42.5.",
                    "identity_file": os.path.basename(ident)},
    "commands": [
        "sbatch --wait experiments/70_hpx_locality_supervision/slice4_root_loss_event/"
        "exp70_slice4_crossnode.sbatch",
        "python -m py_compile run_slice4.py",
        "python run_slice4.py --selftest  (inside allocation)",
        "python run_slice4.py --phase rostam-cross-node --exp68-build-dir <exp68>/build_rostam "
        "--subnet 10.42.5.  (x2, fresh runid/ports/dirs/actors/root each)",
    ],
    "source_hashes": hashes(s4, ["run_slice4.py", "exp70_slice4_crossnode.sbatch",
                                 ".gitignore", "README.md"]),
    "exp68_dependency_hashes": hashes(exp68, ["run_exp68.py", "exp68_peer.cpp", "actor_ext.cpp",
                                              "shared_topk.hpp", "topk_action.hpp",
                                              "CMakeLists.txt"]),
    "exp68_built_artifacts": built,
    "evidence_pointers": {
        "placement": "per-run aggregate.json gates.placement (per-arm hard NodeAffinity "
                     "soft=False attestation) + markers.json crossnode section",
        "in_process_hosting": "per-run aggregate.json gates.<arm>_inprocess",
        "workload": "per-run aggregate.json gates.<arm>_work (bit-exact vs exp68 oracle, both "
                    "coordinator directions), verified in BOTH arms before any transition",
        "result_verification": "per-run aggregate.json gates.<arm>_result_verification",
        "root_injection_preconditions": "per-run aggregate.json gates.loss_injection_"
                                        "preconditions (10 checks before the root is killed)",
        "blindness": "per-run aggregate.json gates.<arm>_blindness + classifier_allowlist; the "
                     "injection record lives at markers.json arms.loss."
                     "injection_record_private, outside the evidence",
        "root_event": "per-run aggregate.json normal_event / loss_event + "
                      "gates.classifier_equivalence",
        "monotonic": "per-run aggregate.json gates.<arm>_monotonic and markers.json "
                     "arms.<arm>.monotonic (expected_refresh_s, last_advance_monotonic, "
                     "classification_bound_s, observed_silence_s, classification_elapsed_s)",
        "pre_bound_negative": "markers.json arms.loss.pre_bound_probe (healthy root observed "
                              "below the bound must NOT yield suspicion)",
        "actor_observations": "per-run aggregate.json gates.loss_actor_observations + "
                              "loss_actor_observation_categories",
        "no_post_event_dispatch": "per-run aggregate.json gates.loss_no_post_event_dispatch",
        "disposal": "per-run aggregate.json gates.<arm>_disposal (loss arm: poisoned-island "
                    "discard, no root.done, no completion witness, root.final absent)",
        "cleanup": "per-run aggregate.json gates.final + orphan_sweep file",
        "arm_isolation": "per-run aggregate.json gates.arm_isolation",
    },
    "runs": runs,
    "artifact_sha256": dict(sorted(artifacts.items())),
}
out = os.path.join(runs_dir, f"hardware_evidence_manifest_{job}.json")
with open(out, "w") as f:
    json.dump(man, f, indent=2, sort_keys=True)
with open(out + ".sha256", "w") as f:
    f.write(f"{sha256(out)}  {os.path.basename(out)}\n")
print(f"[manifest] {out}  runs={len(runs)} closed_artifacts={len(artifacts)} "
      f"post_job_hash_required={man['post_job_hash_required']}")
for r in runs:
    print(f"[manifest] run {r['runid']}: overall={r.get('overall')} "
          f"normal={r.get('normal_event')} loss={r.get('loss_event')} "
          f"failed={r.get('gates_failed')}")
PYEOF

echo "=== done: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
[ "$RUN1" = pass ] && [ "$RUN2" = pass ]
