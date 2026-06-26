# exp52 -- standalone build for the Ray-orchestrated HPX bootstrap clean-island probe.
#
# ISOLATED on purpose: NOT wired into the `_rayx` Python extension build
# (python/CMakeLists.txt) and links no RayX/production code. It builds one HPX
# application binary used for the root and the clean connector.
#
# LAUNCH-HYGIENE (HPX-expert correction): this binary is launched as a CHILD of a Ray
# actor/worker, which may NOT propagate DYLD_LIBRARY_PATH (macOS SIP strips DYLD_* across
# many exec boundaries). So we BAKE an RPATH to the HPX install lib dir into the binary so it
# self-locates libhpx with no loader-env dependency. DYLD_LIBRARY_PATH propagation is a
# fallback only, never the primary mechanism.
#
# Configure with the SAME HPX prefix the extension / exp49-51 use, e.g.:
#   cmake -S experiments/52_ray_bootstrap_clean_island \
#         -B experiments/52_ray_bootstrap_clean_island/build \
#         -G Ninja -DCMAKE_BUILD_TYPE=Release \
#         -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
#   cmake --build experiments/52_ray_bootstrap_clean_island/build

cmake_minimum_required(VERSION 3.19)
project(exp52_ray_bootstrap_clean_island CXX)

find_package(HPX REQUIRED)

# Our own main() calls hpx::init / hpx::start (needed for runtime_mode::connect on the
# connector), so we link HPX::hpx as a library and do NOT use HPX::wrap_main.
add_executable(clean_island_spike clean_island_spike.cpp)
target_link_libraries(clean_island_spike PRIVATE HPX::hpx)
target_compile_features(clean_island_spike PRIVATE cxx_std_17)

# --- Self-locating RPATH (do not depend on DYLD_LIBRARY_PATH inside a Ray worker) ---
set(_hpx_libdirs "/Users/unick/Desktop/Repos/hpx-install/lib")
if(EXISTS "/Users/unick/Desktop/Repos/hpx-install/lib64")
  list(APPEND _hpx_libdirs "/Users/unick/Desktop/Repos/hpx-install/lib64")
endif()
set_target_properties(clean_island_spike PROPERTIES
  BUILD_RPATH "${_hpx_libdirs}"
  INSTALL_RPATH "${_hpx_libdirs}"
  INSTALL_RPATH_USE_LINK_PATH ON)
