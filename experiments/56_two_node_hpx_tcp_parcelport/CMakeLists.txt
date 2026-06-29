# exp56 -- standalone build for the Ray-free two-node HPX TCP parcelport connect-mode probe.
#
# ISOLATED on purpose: this is NOT wired into the `_rayx` Python extension build
# (python/CMakeLists.txt) and links no RayX/production code. It builds one HPX application
# binary used for both roles (root, connect), so the fixed action is registered identically
# in every locality.
#
# Configure with the SAME HPX prefix the extension uses, e.g.:
#   cmake -S experiments/56_two_node_hpx_tcp_parcelport \
#         -B experiments/56_two_node_hpx_tcp_parcelport/build \
#         -G Ninja -DCMAKE_BUILD_TYPE=Release \
#         -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
#   cmake --build experiments/56_two_node_hpx_tcp_parcelport/build
#
# On Rostam, point CMAKE_PREFIX_PATH at the module/install HPX prefix that advertises the TCP
# parcelport (HPX_WITH_PARCELPORT_TCP=ON). See run_two_node_tcp.py `check-config` for verification.

cmake_minimum_required(VERSION 3.19)
project(exp56_two_node_hpx_tcp_parcelport CXX)

find_package(HPX REQUIRED)

# We provide our own main() that calls hpx::init / hpx::start (needed to set runtime_mode::connect
# on the late joiner), so we link HPX::hpx as a library and do NOT use HPX::wrap_main.
add_executable(two_node_tcp_spike two_node_tcp_spike.cpp)
target_link_libraries(two_node_tcp_spike PRIVATE HPX::hpx)
target_compile_features(two_node_tcp_spike PRIVATE cxx_std_17)
