# exp65 -- standalone build for the demand-triggered connect-mode admission spike.
#
# ISOLATED on purpose: NOT wired into the `_rayx` Python extension build
# (python/CMakeLists.txt) and links no RayX/production code. One HPX application
# binary serves both roles (root / connector).
#
# Configure with the SAME HPX prefix the extension uses, e.g.:
#   cmake -S experiments/65_demand_admission \
#         -B experiments/65_demand_admission/build \
#         -G Ninja -DCMAKE_BUILD_TYPE=Release \
#         -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
#   cmake --build experiments/65_demand_admission/build

cmake_minimum_required(VERSION 3.19)
project(exp65_demand_admission CXX)

find_package(HPX REQUIRED)

# Own main() calling hpx::init / hpx::start (needed to set runtime_mode::connect on the
# connector), so link HPX::hpx as a library and do NOT use HPX::wrap_main.
add_executable(demand_admission_spike demand_admission_spike.cpp)
target_link_libraries(demand_admission_spike PRIVATE HPX::hpx)
target_compile_features(demand_admission_spike PRIVATE cxx_std_17)
