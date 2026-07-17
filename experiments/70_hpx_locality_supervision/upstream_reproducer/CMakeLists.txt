# exp70 upstream_reproducer -- standalone, EXPERIMENT-ONLY build (HPX-only).
#
# Minimal two-process loopback reproducer for the exp63 connector-lifetime problem. Buildable
# independently of the RayX repository: no Ray, no Python, no pybind11, no RayX runtime
# libraries, no repo helper outside this directory.
#
# Builds TWO targets:
#   * exp70_root       -- embedded HPX root (hpx::start from a plain main, late connectors
#                         enabled via --hpx:expect-connecting-localities on its command line).
#   * exp70_connector  -- connect-mode locality (hpx::start with runtime_mode::connect).
#
# Action-registration discipline (exp63 model): common.hpp defines + HPX_PLAIN_ACTION-registers
# exp70_probe_action and is included in EXACTLY ONE TU PER BINARY (root.cpp, connector.cpp), so
# the action registers once per binary.
#
# Both binaries provide their own main() and call hpx::start, so they link HPX::hpx and do NOT
# use HPX::wrap_main (same rationale as exp61/62/63/65).
#
# Configure/build:
#   cmake -S . -B build -DHPX_DIR=/path/to/hpx/lib/cmake/HPX
#   cmake --build build

cmake_minimum_required(VERSION 3.19)
project(exp70_upstream_reproducer CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
  message(STATUS "exp70: CMAKE_BUILD_TYPE defaulted to Release")
endif()

# Keep IPO/LTO OFF -- avoid cross-module LTO surprises with HPX in an experiment build.
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)

find_package(HPX REQUIRED)

add_executable(exp70_root root.cpp)
target_link_libraries(exp70_root PRIVATE HPX::hpx)
target_compile_features(exp70_root PRIVATE cxx_std_17)
set_target_properties(exp70_root PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)

add_executable(exp70_connector connector.cpp)
target_link_libraries(exp70_connector PRIVATE HPX::hpx)
target_compile_features(exp70_connector PRIVATE cxx_std_17)
set_target_properties(exp70_connector PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
