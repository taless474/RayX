# exp66 -- standalone, EXPERIMENT-ONLY build for the HPX-inside-one-Ray-actor probe.
#
# Builds TWO targets against the VERIFIED waiter-fix HPX build (exp64 Slice 5V policy):
#   * exp66_actor_ext -- pybind11 module the Ray actor imports; hosts a connect-mode
#                        NETWORKING HPX runtime in-process (hpx::start, no child process).
#   * exp66_peer      -- standalone island peers: --role root (WORK-FREE supervised root)
#                        and --role prober (connect-mode external prober).
#
# Action-registration discipline: probe_action.hpp is included in EXACTLY ONE TU PER BINARY.
# Not wired into python/CMakeLists.txt; links no RayX/production code; NOT the _rayx extension.
#
# Configure (macOS local, fixed verified build):
#   cmake -S experiments/66_hpx_runtime_inside_ray_actor \
#         -B experiments/66_hpx_runtime_inside_ray_actor/build -G Ninja \
#         -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
#         -DCMAKE_PREFIX_PATH="/Users/unick/Desktop/Repos/hpx-master-install;$(python -m pybind11 --cmakedir)"
#   cmake --build experiments/66_hpx_runtime_inside_ray_actor/build

cmake_minimum_required(VERSION 3.19)
project(exp66_hpx_runtime_inside_ray_actor CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
endif()
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)

find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(exp66_actor_ext actor_ext.cpp)
target_link_libraries(exp66_actor_ext PRIVATE HPX::hpx)
target_compile_features(exp66_actor_ext PRIVATE cxx_std_17)
set_target_properties(exp66_actor_ext PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)

add_executable(exp66_peer exp66_peer.cpp)
target_link_libraries(exp66_peer PRIVATE HPX::hpx)
target_compile_features(exp66_peer PRIVATE cxx_std_17)
set_target_properties(exp66_peer PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
