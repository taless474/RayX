cmake_minimum_required(VERSION 3.19)
project(hpx_synthetic_baseline CXX)

# Build against an installed HPX prefix via -DCMAKE_PREFIX_PATH=<hpx-install>.
find_package(HPX REQUIRED)

# HPX::wrap_main lets us keep a normal main(int, char**) that runs inside the
# HPX runtime, so --hpx:* flags (e.g. --hpx:threads=4) are parsed by HPX and
# our own flags pass through untouched.
add_executable(hpx_synthetic_baseline hpx_synthetic_baseline.cpp)
target_link_libraries(hpx_synthetic_baseline PRIVATE HPX::hpx HPX::wrap_main)

# Isolated lane-primitive feasibility probe (experiments/15). Standalone: it
# reuses service_lane.hpp unmodified to measure sleep overshoot and no-op
# dispatch throughput, and is NOT part of the serving-control benchmark corpus.
add_executable(hpx_lane_feasibility hpx_lane_feasibility.cpp)
target_link_libraries(hpx_lane_feasibility PRIVATE HPX::hpx HPX::wrap_main)

# HPX task/dataflow mechanism probe (experiments/20). Standalone: it reuses
# service_lane.hpp and hpx_lane.hpp UNMODIFIED to compare serialized lanes against
# hpx::async / hpx::dataflow pool mechanisms and report which lane contracts are
# preserved/relaxed/n-a. NOT part of the serving-control benchmark corpus.
add_executable(hpx_task_dataflow_probe hpx_task_dataflow_probe.cpp)
target_link_libraries(hpx_task_dataflow_probe PRIVATE HPX::hpx HPX::wrap_main)
