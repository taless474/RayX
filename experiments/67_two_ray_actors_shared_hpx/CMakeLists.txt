# exp67 -- standalone, EXPERIMENT-ONLY build for the two-Ray-actors-share-one-HPX probe.
#
# Builds TWO targets against the VERIFIED waiter-fix HPX build (exp64 Slice 5V / exp66 policy):
#   * exp67_actor_ext -- pybind11 module BOTH Ray actors import; each hosts a connect-mode
#                        NETWORKING HPX runtime in-process (hpx::start, no child process) and
#                        dispatches pid/probe/host actions AT the peer actor's locality.
#   * exp67_peer      -- standalone WORK-FREE supervised root (--role root); no prober role.
#
# Action-registration discipline: probe_action.hpp is included in EXACTLY ONE TU PER BINARY.
# Not wired into python/CMakeLists.txt; links no RayX/production code; NOT the _rayx extension.
#
# Configure (macOS local, fixed verified build):
#   cmake -S experiments/67_two_ray_actors_shared_hpx \
#         -B experiments/67_two_ray_actors_shared_hpx/build -G Ninja \
#         -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
#         -DCMAKE_PREFIX_PATH="/Users/unick/Desktop/Repos/hpx-master-install;$(python -m pybind11 --cmakedir)"
#   cmake --build experiments/67_two_ray_actors_shared_hpx/build

cmake_minimum_required(VERSION 3.19)
project(exp67_two_ray_actors_shared_hpx CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
endif()
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)

find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(exp67_actor_ext actor_ext.cpp)
target_link_libraries(exp67_actor_ext PRIVATE HPX::hpx)
target_compile_features(exp67_actor_ext PRIVATE cxx_std_17)
set_target_properties(exp67_actor_ext PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)

add_executable(exp67_peer exp67_peer.cpp)
target_link_libraries(exp67_peer PRIVATE HPX::hpx)
target_compile_features(exp67_peer PRIVATE cxx_std_17)
set_target_properties(exp67_peer PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
