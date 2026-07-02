# exp64 -- standalone, EXPERIMENT-ONLY build for the payload-fanin size-sweep experiment (Slice 1).
#
# COPIED/ADAPTED from exp63 CMakeLists.txt (targets collective_ext/collective_connector ->
# payload_ext/payload_connector, EXP63_* -> EXP64_*). Self-contained under exp64; references NO
# exp62/exp63 file.
#
# Builds TWO targets:
#   * `payload_ext`       -- pybind11 module embedding an HPX runtime in the Python process. It is the
#                            AGAS ROOT that dispatches exp64_payload_leaf_action leaves (each returning
#                            a serialize_buffer<char> RESPONSE payload) to joined connectors and gathers
#                            them via root_flat_gather_poll (naive all-to-root gather baseline).
#   * `payload_connector` -- standalone connect-mode HPX locality (remote node). It joins the embedded
#                            root's AGAS and serves the SAME exp64_payload_leaf_action. Run on additional
#                            nodes only on a real Slurm allocation; locally it just compiles/links and
#                            supports --role=probe-info.
#
# Action-registration discipline: shared_payload.hpp is pure oracle; payload_action.hpp defines +
# HPX_PLAIN_ACTION-registers exp64_payload_leaf_action and is included in EXACTLY ONE TU PER BINARY
# (payload_ext.cpp and payload_connector.cpp), so the action registers once per binary -- no double
# registration. The exp64-specific action name can never collide with exp62/exp63 actions.
#
# Not wired into python/CMakeLists.txt; links no RayX/production code; NOT the `_rayx` extension. NO Ray.
#
# Configure with the SAME HPX prefix the _rayx extension uses, plus the venv pybind11 cmakedir and
# PYBIND11_FINDPYTHON=ON, e.g. (Rostam):
#   cmake -S experiments/64_payload_fanin_size_sweep \
#         -B experiments/64_payload_fanin_size_sweep/build -G Ninja \
#         -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
#         -DCMAKE_PREFIX_PATH="<hpx-install>;$(python -m pybind11 --cmakedir)"
#   cmake --build experiments/64_payload_fanin_size_sweep/build

cmake_minimum_required(VERSION 3.19)
project(exp64_payload_fanin_size_sweep CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
  message(STATUS "exp64: CMAKE_BUILD_TYPE defaulted to Release")
endif()

# Keep IPO/LTO OFF by default -- avoid cross-module LTO surprises with HPX in an experiment build.
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)

find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

# A Python extension is a shared library with no main(); HPX is started via hpx::start (link
# HPX::hpx, NOT HPX::wrap_main), exactly like exp61/62/63 and the _rayx extension.
pybind11_add_module(payload_ext payload_ext.cpp)
target_link_libraries(payload_ext PRIVATE HPX::hpx)
target_compile_features(payload_ext PRIVATE cxx_std_17)
set_target_properties(payload_ext PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
target_compile_definitions(payload_ext PRIVATE
  EXP64_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  EXP64_COMPILER_ID="${CMAKE_CXX_COMPILER_ID}"
  EXP64_COMPILER_VERSION="${CMAKE_CXX_COMPILER_VERSION}")

# The standalone connect-mode remote locality. We provide our own main() that calls hpx::start with
# runtime_mode::connect (the late joiner), so we link HPX::hpx as a library and do NOT use
# HPX::wrap_main -- same rationale as exp61/62/63. It registers the SAME action via payload_action.hpp,
# but as a SEPARATE binary, so HPX_PLAIN_ACTION registers it once per binary.
add_executable(payload_connector payload_connector.cpp)
target_link_libraries(payload_connector PRIVATE HPX::hpx)
target_compile_features(payload_connector PRIVATE cxx_std_17)
set_target_properties(payload_connector PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
target_compile_definitions(payload_connector PRIVATE
  EXP64_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  EXP64_COMPILER_ID="${CMAKE_CXX_COMPILER_ID}"
  EXP64_COMPILER_VERSION="${CMAKE_CXX_COMPILER_VERSION}")
