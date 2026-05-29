# HPX Build Notes

Reproducible record of the local HPX source install used by RayX. HPX lives
**outside** the RayX repo (sibling directories); only the install prefix is
consumed by the RayX build.

## Result (verified)

* HPX version: **1.11.0** (tag `v1.11.0`)
* Install prefix: `/Users/unick/Desktop/Repos/hpx-install`
* `find_package(HPX REQUIRED)` resolves against the prefix (`HPX_FOUND=1`).
* Platform: macOS arm64 (Apple Silicon), Apple clang 17, CMake 4.3.1, Ninja.

## Paths

| Purpose | Path |
|---|---|
| Source | `/Users/unick/Desktop/Repos/hpx-src` |
| Build  | `/Users/unick/Desktop/Repos/hpx-src/build` |
| Install prefix | `/Users/unick/Desktop/Repos/hpx-install` |

`hpx-src` and `hpx-src/build` are disposable; the install prefix is the only
durable artifact RayX depends on.

## Dependencies (Homebrew)

```bash
brew install boost hwloc
```

Resolved at build time: Boost 1.90.0 (`/opt/homebrew/opt/boost`), hwloc 2.13.0
(`/opt/homebrew/opt/hwloc`). Asio was fetched by HPX (`HPX_WITH_FETCH_ASIO=ON`,
tag `asio-1-21-0`). No vcpkg.

## Clone

```bash
git clone --depth 1 --branch v1.11.0 \
  https://github.com/STEllAR-GROUP/hpx.git /Users/unick/Desktop/Repos/hpx-src
```

## Configure

```bash
cmake -S /Users/unick/Desktop/Repos/hpx-src \
      -B /Users/unick/Desktop/Repos/hpx-src/build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/Users/unick/Desktop/Repos/hpx-install \
  -DHPX_WITH_FETCH_ASIO=ON \
  -DHPX_WITH_MALLOC=system \
  -DHPX_WITH_EXAMPLES=OFF \
  -DHPX_WITH_TESTS=OFF \
  -DHPX_WITH_DOCUMENTATION=OFF \
  -DBOOST_ROOT="$(brew --prefix boost)" \
  -DHWLOC_ROOT="$(brew --prefix hwloc)"
```

Notes:
* HPX auto-enabled `HPX_WITH_GENERIC_CONTEXT_COROUTINES=ON` (expected/safe on
  arm64); not passed manually.
* C++ standard resolved to 17.

## Build and install

```bash
cmake --build /Users/unick/Desktop/Repos/hpx-src/build      # 494 targets, long
cmake --install /Users/unick/Desktop/Repos/hpx-src/build
```

## Verify (find_package probe)

```bash
mkdir -p /tmp/hpxprobe
printf 'cmake_minimum_required(VERSION 3.18)\nproject(p CXX)\nfind_package(HPX REQUIRED)\nmessage(STATUS "HPX_FOUND=${HPX_FOUND} HPX_VERSION=${HPX_VERSION}")\n' \
  > /tmp/hpxprobe/CMakeLists.txt
cmake -S /tmp/hpxprobe -B /tmp/hpxprobe/build \
      -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
```

## Using the install from RayX (next slice)

The future `hpx_impl/CMakeLists.txt` will use:

```bash
cmake -S hpx_impl -B hpx_impl/build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH=/Users/unick/Desktop/Repos/hpx-install
```

## Benign warnings observed (not errors)

* `ranlib: ... table of contents is empty` — header-only/interface modules
  produce empty static archives on macOS.
* `ld: warning: ignoring duplicate libraries: 'lib/libhpx_*.a'` — duplicate
  entries on the link line; deduped by the linker.
* `hpx_wrap.cpp: referring to 'main' ... is a Clang extension [-Wmain]` — HPX's
  main-wrapping shim under clang.
* Configure-time `HPX_WITH_CXX11_ATOMIC_128BIT_RUN_RESULT=0` (arm64) did not
  cause any build problem.
