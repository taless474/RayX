# exp50 -- standalone build for the Ray-free strong-L4 ungraceful connect-loss probe.
#
# ISOLATED on purpose: NOT wired into the `_rayx` Python extension build
# (python/CMakeLists.txt) and links no RayX/production code. It builds one HPX
# application binary used for the root and both connector kinds.
#
# Configure with the SAME HPX prefix the extension / exp49 uses, e.g.:
#   cmake -S experiments/50_strong_l4_connect_failure \
#         -B experiments/50_strong_l4_connect_failure/build \
#         -G Ninja -DCMAKE_BUILD_TYPE=Release \
#         -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
#   cmake --build experiments/50_strong_l4_connect_failure/build

cmake_minimum_required(VERSION 3.19)
project(exp50_strong_l4_connect_failure CXX)

find_package(HPX REQUIRED)

# Our own main() calls hpx::init / hpx::start (needed for runtime_mode::connect on the
# connector), so we link HPX::hpx as a library and do NOT use HPX::wrap_main.
add_executable(dist_fail_spike dist_fail_spike.cpp)
target_link_libraries(dist_fail_spike PRIVATE HPX::hpx)
target_compile_features(dist_fail_spike PRIVATE cxx_std_17)
