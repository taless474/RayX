# exp69 -- standalone, EXPERIMENT-ONLY build for the same-axis top-k performance probe.
#
# Builds TWO targets against the VERIFIED waiter-fix HPX build (exp64 Slice 5V / exp66-68 policy):
#   * exp69_actor_ext -- pybind11 module BOTH Ray actors import; each hosts a connect-mode HPX
#                        locality in-process (hpx::start, no child) and exposes BOTH same-axis
#                        arms' native pieces (HPX coordinate + Ray-arm local_topk_pairs /
#                        merge_topk_native) over ONE shared local_topk/merge_topk implementation.
#   * exp69_peer      -- standalone WORK-FREE supervised root (--role root); never on a timed path.
#
# Action-registration discipline: topk_action.hpp is included in EXACTLY ONE TU PER BINARY.
# Not wired into python/CMakeLists.txt; links no RayX/production code; NOT the _rayx extension.
#
# Configure (macOS local, fixed verified build):
#   cmake -S experiments/69_same_axis_topk_perf \
#         -B experiments/69_same_axis_topk_perf/build -G Ninja \
#         -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
#         -DCMAKE_PREFIX_PATH="/Users/unick/Desktop/Repos/hpx-master-install;$(python -m pybind11 --cmakedir)"
#   cmake --build experiments/69_same_axis_topk_perf/build

cmake_minimum_required(VERSION 3.19)
project(exp69_same_axis_topk_perf CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
endif()
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)

find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(exp69_actor_ext actor_ext.cpp)
target_link_libraries(exp69_actor_ext PRIVATE HPX::hpx)
target_compile_features(exp69_actor_ext PRIVATE cxx_std_17)
set_target_properties(exp69_actor_ext PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)

add_executable(exp69_peer exp69_peer.cpp)
target_link_libraries(exp69_peer PRIVATE HPX::hpx)
target_compile_features(exp69_peer PRIVATE cxx_std_17)
set_target_properties(exp69_peer PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
