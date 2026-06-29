# exp53 -- standalone build for the Ray-supervised whole-island restart probe.
#
# ISOLATED on purpose: NOT wired into the `_rayx` Python extension build
# (python/CMakeLists.txt) and links no RayX/production code. It builds one HPX
# application binary used for the root (failure/clean modes) and both connector
# kinds (victim/clean) across two independent HPX islands.
#
# LAUNCH-HYGIENE (exp52 correction): each HPX child is launched as a CHILD of a Ray
# actor/worker, which may NOT propagate DYLD_LIBRARY_PATH (macOS SIP strips DYLD_* across
# many exec boundaries). We BAKE an RPATH to the HPX install lib dir into the binary so it
# self-locates libhpx with no loader-env dependency. DYLD_LIBRARY_PATH propagation is a
# fallback only, never the primary mechanism.
#
# Configure with the SAME HPX prefix the extension / exp49-52 use, e.g.:
#   cmake -S experiments/53_ray_supervised_island_restart \
#         -B experiments/53_ray_supervised_island_restart/build \
#         -G Ninja -DCMAKE_BUILD_TYPE=Release \
#         -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
#   cmake --build experiments/53_ray_supervised_island_restart/build

cmake_minimum_required(VERSION 3.19)
project(exp53_ray_supervised_island_restart CXX)

find_package(HPX REQUIRED)

# Our own main() calls hpx::init / hpx::start (needed for runtime_mode::connect on the
# connector), so we link HPX::hpx as a library and do NOT use HPX::wrap_main.
add_executable(island_restart_spike island_restart_spike.cpp)
target_link_libraries(island_restart_spike PRIVATE HPX::hpx)
target_compile_features(island_restart_spike PRIVATE cxx_std_17)

# --- Self-locating RPATH (do not depend on DYLD_LIBRARY_PATH inside a Ray worker) ---
set(_hpx_libdirs "/Users/unick/Desktop/Repos/hpx-install/lib")
if(EXISTS "/Users/unick/Desktop/Repos/hpx-install/lib64")
  list(APPEND _hpx_libdirs "/Users/unick/Desktop/Repos/hpx-install/lib64")
endif()
set_target_properties(island_restart_spike PROPERTIES
  BUILD_RPATH "${_hpx_libdirs}"
  INSTALL_RPATH "${_hpx_libdirs}"
  INSTALL_RPATH_USE_LINK_PATH ON)
