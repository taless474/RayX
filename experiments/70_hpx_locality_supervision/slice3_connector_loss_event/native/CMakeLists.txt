# exp70 Slice 3B -- standalone, EXPERIMENT-ONLY build against upstream HPX supervision +
# force_disconnect (HPX issue #7390 / #7441 / PR #7447).
#
# Builds TWO targets, reusing the Slice 3 topology (one work-free standalone root locality +
# Ray-actor-hosted connect-mode connectors, exp66/67/68 lineage):
#   * exp70_slice3b_root -- standalone root (locality 0). Calls hpx::supervision::init(),
#                           discover_and_join(), check_admission(), query_state(), and
#                           hpx::force_disconnect() explicitly. Never publishes event::failed.
#   * exp70_slice3b_ext  -- pybind11 module each Ray actor imports; hosts a connect-mode HPX
#                           locality in-process (exp67 mechanism) and calls
#                           hpx::supervision::init() so the root's discover_and_join() can
#                           join it.
#
# HARD REQUIREMENT (do not silently assume): this experiment specifically validates
# hpx::supervision + hpx::force_disconnect, both gated upstream behind HPX_WITH_SUPERVISION
# (which also enables HPX_HAVE_FORCE_DISCONNECT; see hpx-master-src/CMakeLists.txt). A HPX
# build configured without it cannot exercise this slice at all, so configuration fails loudly
# here rather than producing a binary that silently degrades or no-ops.
#
# Configure/build (Rostam; HPX must be rebuilt with -DHPX_WITH_SUPERVISION=ON first -- see
# ../../README.md / the exp70 evidence index for the exact Rostam commands):
#   cmake -S experiments/70_hpx_locality_supervision/slice3_connector_loss_event/native \
#         -B experiments/70_hpx_locality_supervision/slice3_connector_loss_event/native/build \
#         -G Ninja -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON \
#         -DCMAKE_PREFIX_PATH="<hpx-install-with-supervision>;$(python -m pybind11 --cmakedir)"
#   cmake --build experiments/70_hpx_locality_supervision/slice3_connector_loss_event/native/build

cmake_minimum_required(VERSION 3.19)
project(exp70_slice3b CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
endif()
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)

find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

if(NOT HPX_WITH_SUPERVISION)
  message(FATAL_ERROR
    "exp70 Slice 3B requires an HPX build configured with -DHPX_WITH_SUPERVISION=ON "
    "(this also enables HPX_HAVE_FORCE_DISCONNECT; see hpx-master-src/CMakeLists.txt around "
    "the HPX_WITH_SUPERVISION option). The HPX package found via HPX_DIR/CMAKE_PREFIX_PATH "
    "reports HPX_WITH_SUPERVISION='${HPX_WITH_SUPERVISION}'. Rebuild HPX with this flag "
    "enabled, or point HPX_DIR/CMAKE_PREFIX_PATH at a build that has it. This is a hard "
    "requirement, not a skip: Slice 3B's whole point is exercising this upstream capability.")
endif()

if(NOT TARGET HPX::supervision_dispatch_component)
  message(FATAL_ERROR
    "exp70 Slice 3B requires the HPX::supervision_dispatch_component target, which the found "
    "HPX package does not export even though HPX_WITH_SUPERVISION='${HPX_WITH_SUPERVISION}'. "
    "Confirm HPX_DIR/CMAKE_PREFIX_PATH points at a build+install that was actually "
    "reconfigured and rebuilt after enabling HPX_WITH_SUPERVISION (a stale install directory "
    "from an older configure will not have this target).")
endif()

message(STATUS
  "exp70 Slice 3B: HPX_WITH_SUPERVISION=${HPX_WITH_SUPERVISION}, "
  "HPX::supervision_dispatch_component found -- building against real upstream supervision.")

add_executable(exp70_slice3b_root root_supervised.cpp)
target_link_libraries(exp70_slice3b_root PRIVATE HPX::hpx HPX::supervision_dispatch_component)
target_compile_features(exp70_slice3b_root PRIVATE cxx_std_17)
set_target_properties(exp70_slice3b_root PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)

pybind11_add_module(exp70_slice3b_ext connector_ext.cpp)
target_link_libraries(exp70_slice3b_ext PRIVATE HPX::hpx HPX::supervision_dispatch_component)
target_compile_features(exp70_slice3b_ext PRIVATE cxx_std_17)
set_target_properties(exp70_slice3b_ext PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
