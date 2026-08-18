#!/bin/bash
#SBATCH --job-name=exp70s3b
#SBATCH --partition=medusa
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=03:00:00
#SBATCH --output=/work/bitayekrang/RayX/experiments/70_hpx_locality_supervision/slice3_connector_loss_event/_exp70_slice3b_runs/rostam_build_%j.out

set -uo pipefail

echo "=== SLURM IDENTITY ==="
echo "SLURM_JOB_ID=$SLURM_JOB_ID"
echo "SLURM_JOB_NODELIST=$SLURM_JOB_NODELIST"
scontrol show hostnames "$SLURM_JOB_NODELIST"
hostname
nproc

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
export SLURM_EXPORT_ENV=ALL

RAYX=/work/bitayekrang/RayX
EXP70=$RAYX/experiments/70_hpx_locality_supervision
SLICE3=$EXP70/slice3_connector_loss_event
RUNS=$SLICE3/_exp70_slice3b_runs
mkdir -p "$RUNS"

HPX_SRC=/work/bitayekrang/source/hpx-supervision-src
HPX_BUILD=/work/bitayekrang/builds/hpx-supervision-build
HPX_INSTALL=/work/bitayekrang/apps/hpx-supervision-install
PINNED_COMMIT=7b88345b6c72dfe1dabce1c0398e021f5ca55a4f

echo "=== PHASE 1: HPX source (pinned commit, NOT touching the canonical hpx-master-20bc3d4b install) ==="
if [ ! -d "$HPX_SRC/.git" ]; then
  git clone https://github.com/TheHPXProject/hpx.git "$HPX_SRC"
fi
cd "$HPX_SRC"
git fetch origin master
git checkout "$PINNED_COMMIT"
echo "HPX_SRC_COMMIT=$(git rev-parse HEAD)"
echo "HPX_SRC_COMMIT_DATE=$(git log -1 --format=%cI)"

echo "=== PHASE 2: configure HPX with supervision ==="
# Idempotency check uses the INSTALLED artifact, not a cache flag: CMake writes cache entries
# incrementally even on a failed configure (e.g. the prior attempt's Asio FetchContent failure
# still left HPX_WITH_SUPERVISION=ON in a half-configured cache), so a cache-flag check alone is
# unreliable. Only a completed `--target install` proves a working build exists.
CANONICAL_DEPS=/work/bitayekrang/builds/hpx-master-build/_deps
NEED_BUILD=1
if [ -f "$HPX_INSTALL/lib64/cmake/HPX/HPXConfig.cmake" ]; then
  NEED_BUILD=0
  echo "existing $HPX_INSTALL already has a completed install, reusing (skip configure+build)"
fi
if [ "$NEED_BUILD" = "1" ]; then
  rm -rf "$HPX_BUILD"
  # FETCHCONTENT_SOURCE_DIR_ASIO/STDEXEC point at the already-fetched dependency trees from the
  # canonical hpx-master-build (asio-1-30-2, same HPX_WITH_ASIO_TAG this repo already uses),
  # sidestepping a CMake FetchContent/Asio fetch failure hit on the first attempt (job 185464)
  # even though the compute node's own internet access was independently confirmed working via
  # srun+curl -- narrow build-dependency-sourcing fix, unrelated to supervision/force_disconnect.
  cmake -S "$HPX_SRC" -B "$HPX_BUILD" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$HPX_INSTALL" \
    -DHPX_WITH_CXX_STANDARD=20 \
    -DHPX_WITH_DISTRIBUTED_RUNTIME=ON \
    -DHPX_WITH_NETWORKING=ON \
    -DHPX_WITH_PARCELPORT_TCP=ON \
    -DHPX_WITH_SUPERVISION=ON \
    -DHPX_WITH_FORCE_DISCONNECT=ON \
    -DHPX_WITH_EXAMPLES=OFF \
    -DHPX_WITH_TESTS=OFF \
    -DHPX_WITH_MALLOC=system \
    -DHPX_WITH_FETCH_ASIO=ON \
    -DFETCHCONTENT_SOURCE_DIR_ASIO="$CANONICAL_DEPS/asio-src" \
    -DHPX_WITH_FETCH_STDEXEC=ON \
    -DFETCHCONTENT_SOURCE_DIR_STDEXEC="$CANONICAL_DEPS/stdexec-src"
  CONFIGURE_RC=$?
  echo "HPX_CONFIGURE_RC=$CONFIGURE_RC"
  if [ "$CONFIGURE_RC" -ne 0 ]; then
    echo "HPX CONFIGURE FAILED -- stopping"
    exit 9
  fi
fi

echo "=== PHASE 2 VERIFY: CMake cache option values (do not assume) ==="
grep -E "^HPX_WITH_SUPERVISION:|^HPX_WITH_FORCE_DISCONNECT:|^HPX_WITH_DISTRIBUTED_RUNTIME:|^HPX_WITH_NETWORKING:|^HPX_WITH_PARCELPORT_TCP:|^CMAKE_BUILD_TYPE:|^CMAKE_CXX_COMPILER:|^CMAKE_GENERATOR:" "$HPX_BUILD/CMakeCache.txt"

echo "=== PHASE 3: build + install HPX (this is the slow step) ==="
HPX_BUILD_RC=0
if [ "$NEED_BUILD" = "1" ]; then
  BUILD_T0=$(date +%s)
  cmake --build "$HPX_BUILD" -j"$(nproc)" --target install
  HPX_BUILD_RC=$?
  BUILD_T1=$(date +%s)
  echo "HPX_BUILD_RC=$HPX_BUILD_RC"
  echo "HPX_BUILD_SECONDS=$((BUILD_T1 - BUILD_T0))"
  if [ "$HPX_BUILD_RC" -ne 0 ]; then
    echo "HPX BUILD FAILED -- stopping"
    exit 10
  fi
else
  echo "skipped (reusing existing install)"
fi

echo "=== PHASE 3 VERIFY: installed supervision_dispatch component + force_disconnect header ==="
find "$HPX_INSTALL" -iname "*supervision_dispatch*" | head -20
grep -n "HPX_HAVE_SUPERVISION\|HPX_HAVE_FORCE_DISCONNECT" "$HPX_INSTALL"/include/hpx/config/*.hpp 2>/dev/null | head -10
test -e "$HPX_INSTALL/lib64/cmake/HPX/HPXConfig.cmake" && echo "HPXConfig.cmake present"
find "$HPX_INSTALL/lib" -iname "*supervision*"

echo "=== PHASE 4: configure + build Slice 3B native harness against the new install ==="
source /work/bitayekrang/venvs/rayx-a2b/bin/activate
PYBIND11_CMAKE_DIR=$(python -m pybind11 --cmakedir)
echo "PYBIND11_CMAKE_DIR=$PYBIND11_CMAKE_DIR"

NATIVE_DIR=$SLICE3/native
NATIVE_BUILD=$NATIVE_DIR/build
rm -rf "$NATIVE_BUILD"
cmake -S "$NATIVE_DIR" -B "$NATIVE_BUILD" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DPYBIND11_FINDPYTHON=ON \
  -DCMAKE_PREFIX_PATH="$HPX_INSTALL;$PYBIND11_CMAKE_DIR"
CONFIGURE_RC=$?
echo "NATIVE_CONFIGURE_RC=$CONFIGURE_RC"
if [ "$CONFIGURE_RC" -ne 0 ]; then
  echo "NATIVE CONFIGURE FAILED -- stopping"
  exit 11
fi

cmake --build "$NATIVE_BUILD" -j"$(nproc)"
NATIVE_BUILD_RC=$?
echo "NATIVE_BUILD_RC=$NATIVE_BUILD_RC"
if [ "$NATIVE_BUILD_RC" -ne 0 ]; then
  echo "NATIVE BUILD FAILED -- stopping"
  exit 12
fi

echo "=== PHASE 4 VERIFY: produced binaries + linkage ==="
ls -la "$NATIVE_BUILD"
echo "--- ldd exp70_slice3b_root ---"
ldd "$NATIVE_BUILD/exp70_slice3b_root" 2>&1 | grep -i hpx
echo "--- ldd exp70_slice3b_ext*.so ---"
for so in "$NATIVE_BUILD"/exp70_slice3b_ext*.so; do
  echo "-- $so --"
  ldd "$so" 2>&1 | grep -i hpx
done

echo "=== PHASE 5: selftest ==="
cd "$SLICE3"
python3 run_slice3b.py --selftest
SELFTEST_RC=$?
echo "SELFTEST_RC=$SELFTEST_RC"

echo "=== PHASE 6: live local run (first attempt, evidence preserved as-is) ==="
python3 run_slice3b.py --phase local --build-dir "$NATIVE_BUILD"
LIVE_RC=$?
echo "LIVE_RC=$LIVE_RC"

echo "=== JOB DONE ==="
echo "FINAL_SUMMARY hpx_build_rc=$HPX_BUILD_RC native_build_rc=$NATIVE_BUILD_RC selftest_rc=$SELFTEST_RC live_rc=$LIVE_RC"
