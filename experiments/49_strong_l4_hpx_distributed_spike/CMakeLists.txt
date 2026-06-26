# exp49 -- standalone build for the Ray-free strong-L4 HPX distributed spike.
#
# ISOLATED on purpose: this is NOT wired into the `_rayx` Python extension build
# (python/CMakeLists.txt) and links no RayX/production code. It builds one HPX
# application binary used for both phases and both roles.
#
# Configure with the SAME HPX prefix the extension uses, e.g.:
#   cmake -S experiments/49_strong_l4_hpx_distributed_spike \
#         -B experiments/49_strong_l4_hpx_distributed_spike/build \
#         -G Ninja -DCMAKE_BUILD_TYPE=Release \
#         -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
#   cmake --build experiments/49_strong_l4_hpx_distributed_spike/build

cmake_minimum_required(VERSION 3.19)
project(exp49_strong_l4_hpx_distributed_spike CXX)

find_package(HPX REQUIRED)

# We provide our own main() that calls hpx::init (needed to set runtime_mode::connect on
# the late joiner), so we link HPX::hpx as a library and do NOT use HPX::wrap_main.
add_executable(dist_probe_spike dist_probe_spike.cpp)
target_link_libraries(dist_probe_spike PRIVATE HPX::hpx)
target_compile_features(dist_probe_spike PRIVATE cxx_std_17)
