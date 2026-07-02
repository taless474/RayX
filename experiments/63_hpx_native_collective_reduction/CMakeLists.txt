# exp63 -- standalone, EXPERIMENT-ONLY build for the HPX progress root-cause micro-slice (Slice 1b).
#
# COPIED/ADAPTED from exp62 CMakeLists.txt (targets fanout_ext/fanout_connector -> collective_ext/
# collective_connector, EXP62_* -> EXP63_*). Self-contained under exp63; references NO exp62 file.
#
# Builds TWO targets:
#   * `collective_ext`       -- pybind11 module embedding an HPX runtime in the Python process. It is
#                               the AGAS ROOT that dispatches exp63_leaf_action leaves to joined
#                               connectors and composes the fan-in per mode (root_flat_gather_poll |
#                               when_all_then_reduce | dataflow_reduce), with the Slice 1b
#                               background-yielder progress probe.
#   * `collective_connector` -- standalone connect-mode HPX locality (remote node). It joins the
#                               embedded root's AGAS and serves the SAME exp63_leaf_action. Run on
#                               additional nodes only on a real Slurm allocation; locally it just
#                               compiles/links and supports --role=probe-info.
#
# Action-registration discipline: shared_collective.hpp is pure oracle; collective_action.hpp defines
# + HPX_PLAIN_ACTION-registers exp63_leaf_action and is included in EXACTLY ONE TU PER BINARY
# (collective_ext.cpp and collective_connector.cpp), so the action registers once per binary -- no
# double registration. The exp63-specific action name can never collide with exp62's exp62_leaf_action.
#
# Not wired into python/CMakeLists.txt; links no RayX/production code; NOT the `_rayx` extension.
# NO Ray anywhere.
#
# Configure with the SAME HPX prefix the _rayx extension uses, plus the venv pybind11 cmakedir and
# PYBIND11_FINDPYTHON=ON, e.g. (Rostam):
#   cmake -S experiments/63_hpx_native_collective_reduction \
#         -B experiments/63_hpx_native_collective_reduction/build -G Ninja \
#         -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
#         -DCMAKE_PREFIX_PATH="<hpx-install>;$(python -m pybind11 --cmakedir)"
#   cmake --build experiments/63_hpx_native_collective_reduction/build

cmake_minimum_required(VERSION 3.19)
project(exp63_hpx_native_collective_reduction CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
  message(STATUS "exp63: CMAKE_BUILD_TYPE defaulted to Release")
endif()

# Keep IPO/LTO OFF by default -- avoid cross-module LTO surprises with HPX in an experiment build.
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)

find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

# A Python extension is a shared library with no main(); HPX is started via hpx::start (link
# HPX::hpx, NOT HPX::wrap_main), exactly like exp61/exp62 and the _rayx extension.
pybind11_add_module(collective_ext collective_ext.cpp)
target_link_libraries(collective_ext PRIVATE HPX::hpx)
target_compile_features(collective_ext PRIVATE cxx_std_17)
set_target_properties(collective_ext PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
target_compile_definitions(collective_ext PRIVATE
  EXP63_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  EXP63_COMPILER_ID="${CMAKE_CXX_COMPILER_ID}"
  EXP63_COMPILER_VERSION="${CMAKE_CXX_COMPILER_VERSION}")

# The standalone connect-mode remote locality. We provide our own main() that calls hpx::start with
# runtime_mode::connect (the late joiner), so we link HPX::hpx as a library and do NOT use
# HPX::wrap_main -- same rationale as exp61/exp62. It registers the SAME action via
# collective_action.hpp, but as a SEPARATE binary, so HPX_PLAIN_ACTION registers it once per binary.
add_executable(collective_connector collective_connector.cpp)
target_link_libraries(collective_connector PRIVATE HPX::hpx)
target_compile_features(collective_connector PRIVATE cxx_std_17)
set_target_properties(collective_connector PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
target_compile_definitions(collective_connector PRIVATE
  EXP63_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  EXP63_COMPILER_ID="${CMAKE_CXX_COMPILER_ID}"
  EXP63_COMPILER_VERSION="${CMAKE_CXX_COMPILER_VERSION}")
