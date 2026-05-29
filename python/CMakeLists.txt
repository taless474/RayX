cmake_minimum_required(VERSION 3.19)
project(rayx_ext CXX)

# Build against an installed HPX prefix and the pybind11 shipped in the venv.
# Configure with, e.g.:
#   cmake -S python -B python/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
#     -DPYBIND11_FINDPYTHON=ON \
#     -DCMAKE_PREFIX_PATH="<hpx-install>;$(.venv/bin/python -m pybind11 --cmakedir)" \
#     -DPython_EXECUTABLE="<repo>/.venv/bin/python"
# NOTE: -DPYBIND11_FINDPYTHON=ON is REQUIRED, otherwise pybind11 falls back to
# the legacy FindPythonInterp and may grab a system Python instead of the venv.
find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

# Python-facing module is `_rayx`. Link HPX::hpx only -- NOT HPX::wrap_main:
# a Python extension is a shared library with no main(), so HPX is started as
# a library (hpx::start / hpx::stop). The module reuses the shared service-lane
# core from ../hpx_impl/service_lane.hpp.
pybind11_add_module(_rayx src/rayx/_rayx.cpp)
target_include_directories(_rayx PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../hpx_impl)
target_link_libraries(_rayx PRIVATE HPX::hpx)

# Drop the compiled module next to the Python package so `PYTHONPATH=python/src`
# can import `rayx._rayx`.
set_target_properties(_rayx PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/rayx)
