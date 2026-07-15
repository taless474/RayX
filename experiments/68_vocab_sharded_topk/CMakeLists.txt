# exp68 -- standalone, EXPERIMENT-ONLY build for the vocab-sharded top-k probe.
#
# Builds TWO targets against the VERIFIED waiter-fix HPX build (exp64 Slice 5V / exp66/67 policy):
#   * exp68_actor_ext -- pybind11 module BOTH Ray actors import; each hosts a connect-mode HPX
#                        locality in-process (hpx::start, no child), owns a vocab shard, and
#                        coordinates a distributed top-k with the peer over HPX actions/futures.
#   * exp68_peer      -- standalone WORK-FREE supervised root (--role root); no prober role.
#
# Action-registration discipline: topk_action.hpp is included in EXACTLY ONE TU PER BINARY.
# Not wired into python/CMakeLists.txt; links no RayX/production code; NOT the _rayx extension.
#
# Configure (macOS local, fixed verified build):
#   cmake -S experiments/68_vocab_sharded_topk \
#         -B experiments/68_vocab_sharded_topk/build -G Ninja \
#         -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
#         -DCMAKE_PREFIX_PATH="/Users/unick/Desktop/Repos/hpx-master-install;$(python -m pybind11 --cmakedir)"
#   cmake --build experiments/68_vocab_sharded_topk/build

cmake_minimum_required(VERSION 3.19)
project(exp68_vocab_sharded_topk CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
endif()
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)

find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(exp68_actor_ext actor_ext.cpp)
target_link_libraries(exp68_actor_ext PRIVATE HPX::hpx)
target_compile_features(exp68_actor_ext PRIVATE cxx_std_17)
set_target_properties(exp68_actor_ext PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)

add_executable(exp68_peer exp68_peer.cpp)
target_link_libraries(exp68_peer PRIVATE HPX::hpx)
target_compile_features(exp68_peer PRIVATE cxx_std_17)
set_target_properties(exp68_peer PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
