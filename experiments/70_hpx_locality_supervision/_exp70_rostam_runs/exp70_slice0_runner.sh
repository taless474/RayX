#!/usr/bin/env bash
# exp70 Slice 0 Linux loopback confirmation runner (EXPERIMENT-ONLY run-support artifact).
# Executed on ONE allocated medusa compute node via: salloc ... srun -N1 -n1 bash -l <this>.
# HPX-only, loopback 127.0.0.1, no Ray, no Python runtime dependency, no multi-node claim,
# no performance claim. Never deletes retained work directories.
set -u

echo "runner: host=$(hostname) job=${SLURM_JOB_ID:-} nodelist=${SLURM_JOB_NODELIST:-}"
case "$(hostname)" in
    medusa*) ;;
    *) echo "STOP: not a medusa compute node"; exit 70 ;;
esac
test -n "${SLURM_JOB_ID:-}" || { echo "STOP: SLURM_JOB_ID empty"; exit 70; }
test -n "${SLURM_JOB_NODELIST:-}" || { echo "STOP: SLURM_JOB_NODELIST empty"; exit 70; }

type module >/dev/null 2>&1 || source /etc/profile.d/modules.sh 2>/dev/null || true
module purge >/dev/null 2>&1
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 || { echo "STOP: module load failed"; exit 70; }
export SLURM_EXPORT_ENV=ALL

EXP70=/work/bitayekrang/RayX/experiments/70_hpx_locality_supervision
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RESULT_ROOT="$EXP70/_exp70_rostam_runs/${STAMP}_slice0_linux_loopback"
mkdir -p "$RESULT_ROOT/work" || { echo "STOP: cannot create result root"; exit 70; }
echo "RESULT_ROOT=$RESULT_ROOT"
cp -- "$0" "$RESULT_ROOT/runner_used.sh" 2>/dev/null || true

{
    echo "hostname=$(hostname)"
    echo "slurm_job_id=$SLURM_JOB_ID"
    echo "slurm_job_nodelist=$SLURM_JOB_NODELIST"
    scontrol show hostnames "$SLURM_JOB_NODELIST"
} >"$RESULT_ROOT/allocation.txt" 2>&1

{
    module list 2>&1
    echo
    echo "which_cxx=$(which c++)"
    c++ --version
    echo
    cmake --version
    echo
    ninja --version 2>/dev/null || echo "ninja not on PATH; fallback generator will be used (exp64 precedent)"
} >"$RESULT_ROOT/env_identity.txt" 2>&1

# ---- HPX identity gate (fail before building) -------------------------------------------------
HPX_PREFIX=/work/bitayekrang/apps/hpx-master-20bc3d4b-install
EXPECTED=20bc3d4bf3068383edcb63be13f22e9ff95842fa
grep HPX_HAVE_GIT_COMMIT "$HPX_PREFIX/include/hpx/config/defines.hpp" >"$RESULT_ROOT/hpx_identity.txt" 2>&1
if ! grep -q "\"$EXPECTED\"" "$HPX_PREFIX/include/hpx/config/defines.hpp"; then
    echo "STOP: installed HPX commit does not match $EXPECTED" | tee -a "$RESULT_ROOT/hpx_identity.txt"
    exit 71
fi
HPX_DIR=$(dirname "$(find "$HPX_PREFIX" -name HPXConfig.cmake 2>/dev/null | head -1)")
if [ -z "$HPX_DIR" ]; then
    echo "STOP: HPXConfig.cmake not found under $HPX_PREFIX" | tee -a "$RESULT_ROOT/hpx_identity.txt"
    exit 72
fi
{
    echo "hpx_prefix=$HPX_PREFIX"
    echo "hpx_dir=$HPX_DIR"
    ls "$HPX_PREFIX"/lib*/libhpx.so* 2>/dev/null
} >>"$RESULT_ROOT/hpx_identity.txt"

# ---- clean Linux-specific build ----------------------------------------------------------------
cd "$EXP70/upstream_reproducer" || { echo "STOP: reproducer dir missing"; exit 73; }
chmod +x run_case.sh
GEN="Unix Makefiles"
command -v ninja >/dev/null 2>&1 && GEN=Ninja
echo "generator=$GEN" >>"$RESULT_ROOT/env_identity.txt"
rm -rf build-rostam-master
cmake -S . -B build-rostam-master -G "$GEN" -DCMAKE_BUILD_TYPE=Release -DHPX_DIR="$HPX_DIR" \
    >"$RESULT_ROOT/configure.log" 2>&1 || { echo "STOP: configure failed (see configure.log)"; exit 74; }
cmake --build build-rostam-master -j 8 >"$RESULT_ROOT/build.log" 2>&1 \
    || { echo "STOP: build failed (see build.log)"; exit 75; }
ls -l build-rostam-master/exp70_root build-rostam-master/exp70_connector >>"$RESULT_ROOT/build.log" 2>&1 \
    || { echo "STOP: binaries missing after build"; exit 76; }

# ---- case 1 then case 2, strictly sequential ---------------------------------------------------
# Loopback under Slurm: HPX batch-env autodetect rejects 127.0.0.1 ("Requested AGAS host not
# found in node list"). Repo lesson (exp52/61/65; exp54 "SCRUBBED to prove it"): never let
# batch-env autodetect override explicit endpoints. The reproducer files stay byte-identical to
# the macOS-validated source; instead each case runs in a subshell with SLURM* env removed.
# Placement proof is unaffected: allocation.txt + the hostname embedded in the runtime markers.
BUILD_ABS="$PWD/build-rostam-master"
env | grep -o '^SLURM[^=]*' | sort >"$RESULT_ROOT/slurm_env_scrubbed_names.txt"

run_case_scrubbed() {
    ( for v in $(compgen -e | grep '^SLURM'); do unset "$v"; done
      BUILD_DIR="$BUILD_ABS" KEEP_WORKDIR=1 TMPDIR="$RESULT_ROOT/work" ./run_case.sh "$1" )
}

run_case_scrubbed late-dispatch-current-behavior \
    >"$RESULT_ROOT/case1.stdout" 2>"$RESULT_ROOT/case1.stderr"
C1RC=$?
echo "case1_rc=$C1RC" >"$RESULT_ROOT/case1.rc"

run_case_scrubbed external-lifecycle-workaround \
    >"$RESULT_ROOT/case2.stdout" 2>"$RESULT_ROOT/case2.stderr"
C2RC=$?
echo "case2_rc=$C2RC" >"$RESULT_ROOT/case2.rc"

# ---- orphan sweep (exp70 binaries only) --------------------------------------------------------
pgrep -fl 'exp70_root|exp70_connector' >"$RESULT_ROOT/orphan_sweep.txt" 2>&1
echo "pgrep_rc=$? (1 means no matching process)" >>"$RESULT_ROOT/orphan_sweep.txt"

# ---- gate evaluation (recorded; also re-checked on the Mac after copy-back) --------------------
GATES1='"overall":"pass" "first_dispatch_succeeded":true "race_constructed":true
"connector_stopped_before_second_dispatch":true "second_dispatch_attempted":true
"second_dispatch_succeeded":false "throw_site":"async_call" "error_type":"std::system_error"
"system_error_code":1 "orphan_count":0 "hpx_git_commit":"'$EXPECTED'"'
{
    for g in $GATES1; do
        grep -qF "$g" "$RESULT_ROOT/case1.stdout" && echo "PASS $g" || echo "FAIL $g"
    done
    echo "-- recorded (not asserted) --"
    grep -o '"system_error_category":"[^"]*"' "$RESULT_ROOT/case1.stdout"
    grep -o '"error_what":"[^"]*"' "$RESULT_ROOT/case1.stdout"
    grep -o '"historical_exact_symptom_reproduced":[a-z]*' "$RESULT_ROOT/case1.stdout"
    grep -o '"elapsed_ms":[0-9]*' "$RESULT_ROOT/case1.stdout"
} >"$RESULT_ROOT/case1.gates.txt" 2>&1

GATES2='"overall":"pass" "first_dispatch_succeeded":true "second_dispatch_succeeded":true
"match":true "proved_remote":true "normal_completion_observed":true
"root_silence_observed":false "late_parcel_evidence_seen":false "orphan_count":0
"hpx_git_commit":"'$EXPECTED'"'
C2WORK=$(sed -n 's/.*"workdir":"\([^"]*\)".*/\1/p' "$RESULT_ROOT/case2.stdout" | head -1)
{
    for g in $GATES2; do
        grep -qF "$g" "$RESULT_ROOT/case2.stdout" && echo "PASS $g" || echo "FAIL $g"
    done
    echo "-- connector completion marker ($C2WORK/connector.stopped) --"
    grep -o '"connector_shutdown_reason":"[^"]*"\|"normal_completion":[a-z]*\|"deadman_expired":[a-z]*\|"witness_advances":[0-9]*' \
        "$C2WORK/connector.stopped" 2>/dev/null
    echo "-- recorded (not asserted) --"
    grep -o '"executed_on":[0-9]*' "$RESULT_ROOT/case2.stdout"
    grep -o '"result":[0-9-]*' "$RESULT_ROOT/case2.stdout"
    grep -o '"oracle":[0-9-]*' "$RESULT_ROOT/case2.stdout"
} >"$RESULT_ROOT/case2.gates.txt" 2>&1

C1WORK=$(sed -n 's/.*"workdir":"\([^"]*\)".*/\1/p' "$RESULT_ROOT/case1.stdout" | head -1)

# ---- manifest ----------------------------------------------------------------------------------
{
    echo "exp70 Slice 0 Linux loopback confirmation"
    echo "date_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "slurm_job_id=$SLURM_JOB_ID"
    echo "nodelist=$SLURM_JOB_NODELIST"
    echo "hostname=$(hostname)"
    echo "modules_compilers=see env_identity.txt"
    echo "hpx_prefix=$HPX_PREFIX"
    echo "hpx_dir=$HPX_DIR"
    grep HPX_HAVE_GIT_COMMIT "$HPX_PREFIX/include/hpx/config/defines.hpp"
    echo "generator=$GEN"
    echo "configure_cmd=cmake -S . -B build-rostam-master -G \"$GEN\" -DCMAKE_BUILD_TYPE=Release -DHPX_DIR=$HPX_DIR"
    echo "build_cmd=cmake --build build-rostam-master -j 8"
    echo "case1_cmd=BUILD_DIR=$BUILD_ABS KEEP_WORKDIR=1 TMPDIR=$RESULT_ROOT/work ./run_case.sh late-dispatch-current-behavior"
    echo "case1_rc=$C1RC"
    echo "case1_workdir=$C1WORK"
    echo "case2_cmd=BUILD_DIR=$BUILD_ABS KEEP_WORKDIR=1 TMPDIR=$RESULT_ROOT/work ./run_case.sh external-lifecycle-workaround"
    echo "case2_rc=$C2RC"
    echo "case2_workdir=$C2WORK"
    echo "slurm_env_scrubbed_for_cases=yes (names in slurm_env_scrubbed_names.txt; loopback under Slurm requires neutralizing HPX batch-env autodetect, exp52/61/65 lesson)"
    echo "scope=loopback 127.0.0.1 only; one allocated node; HPX-only; no Ray; no Python runtime dependency; no multi-node evidence; no performance claim"
} >"$RESULT_ROOT/manifest.txt"

# ---- hashes ------------------------------------------------------------------------------------
( cd "$EXP70/upstream_reproducer" &&
    sha256sum CMakeLists.txt common.hpp root.cpp connector.cpp run_case.sh README.md ) \
    >"$RESULT_ROOT/source_sha256sums.txt"
( cd "$RESULT_ROOT" &&
    sha256sum allocation.txt env_identity.txt hpx_identity.txt configure.log build.log \
        case1.stdout case1.stderr case1.rc case1.gates.txt \
        case2.stdout case2.stderr case2.rc case2.gates.txt \
        orphan_sweep.txt manifest.txt source_sha256sums.txt runner_used.sh \
        slurm_env_scrubbed_names.txt \
        $(find work -type f | sort) \
        >sha256sums.txt 2>/dev/null )

echo "runner: done C1RC=$C1RC C2RC=$C2RC result_root=$RESULT_ROOT"
if [ "$C1RC" -eq 0 ] && [ "$C2RC" -eq 0 ]; then exit 0; else exit 77; fi
