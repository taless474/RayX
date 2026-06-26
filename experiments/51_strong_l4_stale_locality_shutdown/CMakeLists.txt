# exp51 -- standalone build for the Ray-free strong-L4 stale-locality shutdown /
# cleanup / recovery-boundary probe.
#
# ISOLATED on purpose: NOT wired into the `_rayx` Python extension build
# (python/CMakeLists.txt) and links no RayX/production code. It builds one HPX
# application binary used for the root (probes P1/P2/clean_island) and both
# connector kinds (victim/clean).
#
# Configure with the SAME HPX prefix the extension / exp49 / exp50 use, e.g.:
#   cmake -S experiments/51_strong_l4_stale_locality_shutdown \
#         -B experiments/51_strong_l4_stale_locality_shutdown/build \
#         -G Ninja -DCMAKE_BUILD_TYPE=Release \
#         -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
#   cmake --build experiments/51_strong_l4_stale_locality_shutdown/build

cmake_minimum_required(VERSION 3.19)
project(exp51_strong_l4_stale_locality_shutdown CXX)

find_package(HPX REQUIRED)

# Our own main() calls hpx::init / hpx::start (needed for runtime_mode::connect on the
# connector), so we link HPX::hpx as a library and do NOT use HPX::wrap_main.
add_executable(stale_shutdown_spike stale_shutdown_spike.cpp)
target_link_libraries(stale_shutdown_spike PRIVATE HPX::hpx)
target_compile_features(stale_shutdown_spike PRIVATE cxx_std_17)
