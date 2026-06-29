# exp57 -- standalone build for the Ray/Slurm-supervised two-node HPX island arc.
#
# Slice 0 builds ONLY the two-node settle-diagnostic binary (two_node_island_spike), which is a
# diagnostically-instrumented COPY of the exp56 spike. It is NOT wired into the `_rayx` Python
# extension build (python/CMakeLists.txt) and links no RayX/production code. One HPX application
# binary serves both roles (root, connect) so the fixed action is registered identically in every
# locality. Ray is NOT involved in Slice 0 (the diagnostic phase is Ray-free).
#
# Configure with the SAME HPX prefix the extension uses, e.g.:
#   cmake -S experiments/57_ray_slurm_supervised_two_node_island \
#         -B experiments/57_ray_slurm_supervised_two_node_island/build \
#         -G Ninja -DCMAKE_BUILD_TYPE=Release \
#         -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
#   cmake --build experiments/57_ray_slurm_supervised_two_node_island/build
#
# On Rostam, point HPX_DIR / CMAKE_PREFIX_PATH at the module/install HPX prefix that advertises the
# TCP parcelport (HPX_WITH_PARCELPORT_TCP=ON). See run_ray_slurm_supervised_two_node.py `check-config`.

cmake_minimum_required(VERSION 3.19)
project(exp57_ray_slurm_supervised_two_node_island CXX)

find_package(HPX REQUIRED)

# We provide our own main() that calls hpx::init / hpx::start (needed to set runtime_mode::connect
# on the late joiner), so we link HPX::hpx as a library and do NOT use HPX::wrap_main.
add_executable(two_node_island_spike two_node_island_spike.cpp)
target_link_libraries(two_node_island_spike PRIVATE HPX::hpx)
target_compile_features(two_node_island_spike PRIVATE cxx_std_17)
