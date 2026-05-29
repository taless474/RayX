cmake_minimum_required(VERSION 3.19)
project(hpx_synthetic_baseline CXX)

# Build against an installed HPX prefix via -DCMAKE_PREFIX_PATH=<hpx-install>.
find_package(HPX REQUIRED)

# HPX::wrap_main lets us keep a normal main(int, char**) that runs inside the
# HPX runtime, so --hpx:* flags (e.g. --hpx:threads=4) are parsed by HPX and
# our own flags pass through untouched.
add_executable(hpx_synthetic_baseline hpx_synthetic_baseline.cpp)
target_link_libraries(hpx_synthetic_baseline PRIVATE HPX::hpx HPX::wrap_main)
