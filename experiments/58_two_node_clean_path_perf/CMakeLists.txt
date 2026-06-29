# exp58 -- standalone build for the two-node HPX TCP clean-path PERFORMANCE-CHARACTERIZATION spike.
#
# Slice 1 builds ONLY the perf spike (two_node_perf_spike), a copy-in-spirit of the exp57/exp56 two-node
# HPX TCP mechanism with a Class-B timing characterization on the root. It is NOT wired into the `_rayx`
# Python extension build (python/CMakeLists.txt) and links no RayX/production code. One HPX application
# binary serves both roles (root, connect) so the fixed `dist_probe` action is registered identically in
# every locality. NO Ray anywhere.
#
# PERF-TIER POLICY (see perf_characterization.md): Release is the primary valid perf tier;
# RelWithDebInfo is allowed but flagged secondary/debuggable; Debug is INVALID for perf. The build type
# is injected into the binary (EXP58_BUILD_TYPE) and the binary derives perf_build_tier / perf_valid, so
# a Debug build still produces analyzable artifacts but self-labels perf_valid=false. A configure-time
# WARNING is emitted for any non-Release perf-tier build.
#
# Configure with the SAME HPX prefix the extension uses, e.g. (Rostam):
#   cmake -S experiments/58_two_node_clean_path_perf \
#         -B experiments/58_two_node_clean_path_perf/build \
#         -G Ninja -DCMAKE_BUILD_TYPE=Release \
#         -DCMAKE_PREFIX_PATH=<hpx-install-prefix>
#   cmake --build experiments/58_two_node_clean_path_perf/build

cmake_minimum_required(VERSION 3.19)
project(exp58_two_node_clean_path_perf CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
  message(STATUS "exp58: CMAKE_BUILD_TYPE defaulted to Release (primary perf tier)")
endif()

if(NOT CMAKE_BUILD_TYPE STREQUAL "Release")
  if(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    message(WARNING "exp58: RelWithDebInfo is a SECONDARY/debuggable perf tier; do not mix its numbers "
                    "with Release in the same aggregate.")
  else()
    message(WARNING "exp58: CMAKE_BUILD_TYPE='${CMAKE_BUILD_TYPE}' is NOT a valid perf tier "
                    "(Release primary, RelWithDebInfo secondary). The binary will self-label "
                    "perf_valid=false.")
  endif()
endif()

find_package(HPX REQUIRED)

# We provide our own main() that calls hpx::init / hpx::start (needed for runtime_mode::connect on the
# late joiner), so we link HPX::hpx as a library and do NOT use HPX::wrap_main.
add_executable(two_node_perf_spike two_node_perf_spike.cpp)
target_link_libraries(two_node_perf_spike PRIVATE HPX::hpx)
target_compile_features(two_node_perf_spike PRIVATE cxx_std_17)

# Build-identity injected into the binary so each perf artifact records its own provenance.
target_compile_definitions(two_node_perf_spike PRIVATE
  EXP58_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  EXP58_COMPILER_ID="${CMAKE_CXX_COMPILER_ID}"
  EXP58_COMPILER_VERSION="${CMAKE_CXX_COMPILER_VERSION}")
