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

# Cooperative gate/barrier witness spike (experiments/41). Standalone,
# experiment-only: launches N HPX child tasks that arrive, cooperatively suspend
# on a shared hpx::promise<void> gate, and resume when the last arriver opens it.
# Uses only hpx::async / hpx::when_all / hpx::promise<void> / shared_future<void>
# / hpx::this_thread::sleep_for -- NO hpx::latch / barrier / counting_semaphore,
# NO Runtime registry op, NO endpoint bridge. Structural gates only; no timing
# claim. NOT part of the serving-control benchmark corpus.
add_executable(hpx_barrier_witness_spike hpx_barrier_witness_spike.cpp)
target_link_libraries(hpx_barrier_witness_spike PRIVATE HPX::hpx HPX::wrap_main)

# rayx.runtime dispatch-decomposition probe (experiments/25). Standalone
# diagnostic: includes the experimental runtime headers from python/src/rayx
# READ-ONLY and UNMODIFIED (RuntimeLane + op registries; no pybind, no _rayx)
# to decompose per-op dispatch cost and estimate the nested
# hpx::async(...).get() overhead inside the lane worker. Observation-only; NOT
# part of the benchmark corpus, NOT the v1 JSONL schema, no performance verdict.
add_executable(rayx_runtime_dispatch_probe rayx_runtime_dispatch_probe.cpp)
target_include_directories(rayx_runtime_dispatch_probe PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../python/src/rayx)
target_link_libraries(rayx_runtime_dispatch_probe PRIVATE HPX::hpx HPX::wrap_main)
