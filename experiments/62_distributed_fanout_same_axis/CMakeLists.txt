# exp62 -- standalone, EXPERIMENT-ONLY build for the distributed fanout/fanin same-axis experiment.
#
# SLICE 2 builds TWO targets:
#   * `fanout_ext`       -- pybind11 module embedding an HPX runtime in the Python process. Slice 1
#                           single-locality smoke (find_here) + Slice 2 one-remote-locality all-remote
#                           fanout dispatching exp62_leaf_action to a joined connector locality.
#   * `fanout_connector` -- standalone connect-mode HPX locality (the remote node B). It joins the
#                           embedded root's AGAS and serves the SAME exp62_leaf_action as a remote
#                           locality. Run on a second node only on a real Slurm allocation; locally it
#                           just compiles/links and supports --role=probe-info.
#
# Action-registration discipline: shared_fanout.hpp is pure oracle; fanout_action.hpp defines +
# HPX_PLAIN_ACTION-registers exp62_leaf_action and is included in EXACTLY ONE TU PER BINARY
# (fanout_ext.cpp and fanout_connector.cpp), so the action registers once per binary -- no double
# registration.
#
# Not wired into python/CMakeLists.txt; links no RayX/production code; NOT the `_rayx` extension.
# NO Ray anywhere.
#
# Configure with the SAME HPX prefix the _rayx extension uses, plus the venv pybind11 cmakedir and
# PYBIND11_FINDPYTHON=ON, e.g. (local):
#   cmake -S experiments/62_distributed_fanout_same_axis \
#         -B experiments/62_distributed_fanout_same_axis/build -G Ninja \
#         -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
#         -DCMAKE_PREFIX_PATH="<hpx-install>;$(python -m pybind11 --cmakedir)"
#   cmake --build experiments/62_distributed_fanout_same_axis/build

cmake_minimum_required(VERSION 3.19)
project(exp62_distributed_fanout_same_axis CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
  message(STATUS "exp62: CMAKE_BUILD_TYPE defaulted to Release")
endif()

# Keep IPO/LTO OFF by default -- avoid cross-module LTO surprises with HPX in an experiment build.
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)

find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

# A Python extension is a shared library with no main(); HPX is started via hpx::start (link
# HPX::hpx, NOT HPX::wrap_main), exactly like exp61 and the _rayx extension.
pybind11_add_module(fanout_ext fanout_ext.cpp)
target_link_libraries(fanout_ext PRIVATE HPX::hpx)
target_compile_features(fanout_ext PRIVATE cxx_std_17)
set_target_properties(fanout_ext PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
target_compile_definitions(fanout_ext PRIVATE
  EXP62_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  EXP62_COMPILER_ID="${CMAKE_CXX_COMPILER_ID}"
  EXP62_COMPILER_VERSION="${CMAKE_CXX_COMPILER_VERSION}")

# The standalone connect-mode remote locality. We provide our own main() that calls hpx::start with
# runtime_mode::connect (the late joiner), so we link HPX::hpx as a library and do NOT use
# HPX::wrap_main -- same rationale as exp61. It registers the SAME action via fanout_action.hpp, but
# as a SEPARATE binary, so HPX_PLAIN_ACTION registers it exactly once per binary.
add_executable(fanout_connector fanout_connector.cpp)
target_link_libraries(fanout_connector PRIVATE HPX::hpx)
target_compile_features(fanout_connector PRIVATE cxx_std_17)
set_target_properties(fanout_connector PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
target_compile_definitions(fanout_connector PRIVATE
  EXP62_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  EXP62_COMPILER_ID="${CMAKE_CXX_COMPILER_ID}"
  EXP62_COMPILER_VERSION="${CMAKE_CXX_COMPILER_VERSION}")
