# exp61 -- standalone, EXPERIMENT-ONLY build for the same-axis Python-boundary experiment.
#
# Builds:
#   * `dist_probe_ext`        -- pybind11 module embedding an HPX runtime in the Python process and
#                                dispatching the fixed closed-int64 `exp61_dist_probe_action`
#                                (Slice 0: local find_here; Slice 2A: also a cached remote locality).
#   * `dist_probe_connector`  -- standalone connect-mode HPX locality (Slice 2A) that joins the
#                                embedded root's AGAS and serves the SAME action as a remote node.
#
# Neither is wired into python/CMakeLists.txt; both link no RayX/production code and are not the
# `_rayx` extension. NO Ray anywhere. The connector is two-node-capable scaffolding -- it is run on
# a second node only on a real Slurm allocation (Slice 2B); locally it just compiles/links.
#
# Configure with the SAME HPX prefix the _rayx extension uses, plus the venv pybind11 cmakedir,
# and PYBIND11_FINDPYTHON=ON (so pybind11 binds the active interpreter), e.g. (local):
#   cmake -S experiments/61_python_boundary_same_axis_ray_vs_rayx \
#         -B experiments/61_python_boundary_same_axis_ray_vs_rayx/build \
#         -G Ninja -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
#         -DCMAKE_PREFIX_PATH="<hpx-install>;$(python -m pybind11 --cmakedir)"
#   cmake --build experiments/61_python_boundary_same_axis_ray_vs_rayx/build

cmake_minimum_required(VERSION 3.19)
project(exp61_python_boundary_same_axis CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
  message(STATUS "exp61: CMAKE_BUILD_TYPE defaulted to Release")
endif()

find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

# A Python extension is a shared library with no main(); HPX is started via hpx::start (link
# HPX::hpx, NOT HPX::wrap_main), exactly like the _rayx extension.
pybind11_add_module(dist_probe_ext dist_probe_ext.cpp)
target_link_libraries(dist_probe_ext PRIVATE HPX::hpx)
target_compile_features(dist_probe_ext PRIVATE cxx_std_17)
target_compile_definitions(dist_probe_ext PRIVATE
  EXP61_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  EXP61_COMPILER_ID="${CMAKE_CXX_COMPILER_ID}"
  EXP61_COMPILER_VERSION="${CMAKE_CXX_COMPILER_VERSION}")

# Slice 2A: the standalone connect-mode remote locality. We provide our own main() that calls
# hpx::start with runtime_mode::connect (the late joiner), so we link HPX::hpx as a library and do
# NOT use HPX::wrap_main -- same rationale as the exp58 spike. It registers the SAME action via
# shared_dist_probe.hpp, but as a SEPARATE binary, so HPX_PLAIN_ACTION registers it exactly once
# per binary (no double-registration with dist_probe_ext).
add_executable(dist_probe_connector dist_probe_connector.cpp)
target_link_libraries(dist_probe_connector PRIVATE HPX::hpx)
target_compile_features(dist_probe_connector PRIVATE cxx_std_17)
target_compile_definitions(dist_probe_connector PRIVATE
  EXP61_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  EXP61_COMPILER_ID="${CMAKE_CXX_COMPILER_ID}"
  EXP61_COMPILER_VERSION="${CMAKE_CXX_COMPILER_VERSION}")
