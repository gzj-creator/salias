# macOS and Install Support Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make normal named IPC reliable on macOS and provide a consumable CMake install package without changing the public API.

**Architecture:** Encode public channel names into deterministic short POSIX shm names shared by Linux and macOS. Keep Linux huge-page behavior, reject explicit huge pages early on macOS, and export the existing static libraries through a standard CMake package.

**Tech Stack:** C++23, POSIX shm/mmap, CMake 3.25, GoogleTest, CTest

---

### Task 1: Long-name regression

**Files:**
- Modify: `test/api/channel_api_test.cpp`

**Step 1: Write the failing test**

Add a test that constructs a valid 128-character channel name, creates a FIFO MPSC channel, connects to it, publishes one value, and receives it from the connected endpoint.

**Step 2: Run test to verify it fails**

Run: `rtk cmake --build /tmp/salias-macos-build --target salias_api_tests && rtk /tmp/salias-macos-build/test/salias_api_tests --gtest_filter=ChannelApiTest.LongName*`

Expected on macOS before the fix: creation fails with `PlatformFail` because the generated POSIX shm name is too long.

### Task 2: Deterministic short shm names

**Files:**
- Modify: `src/salias/src/channel.cpp`
- Test: `test/api/channel_api_test.cpp`

**Step 1: Implement the minimal encoder**

Add an internal `fnv1a64(std::string_view)` helper and fixed-width lowercase hexadecimal formatting. Change `control_shm_name` and `ring_shm_name` to produce short names from the full public channel name.

**Step 2: Run targeted tests**

Run the long-name test and existing named-channel API tests.

Expected: all targeted tests pass and separate processes derive identical names.

### Task 3: macOS huge-page guard

**Files:**
- Modify: `src/salias/src/channel.cpp`
- Modify: `test/api/channel_api_test.cpp`

**Step 1: Write the platform regression test**

Under `#if defined(__APPLE__)`, request a valid 2 MiB huge-page channel and expect `Error::PlatformFail`.

**Step 2: Verify the test fails for the intended reason**

Run only the new test and confirm the current implementation attempts the hugetlbfs path.

**Step 3: Add early capability rejection**

Reject non-`None` huge-page configs before creating the control shm on non-Linux platforms.

**Step 4: Run targeted tests**

Expected: the platform test and existing huge-page validation tests pass.

### Task 4: CMake install package

**Files:**
- Modify: `CMakeLists.txt`
- Modify: `src/core/CMakeLists.txt`
- Modify: `src/salias/CMakeLists.txt`
- Create: `cmake/saliasConfig.cmake.in`
- Modify: `README.md`

**Step 1: Add install and export rules**

Give both libraries stable export names, add install include interfaces, install public headers and archives, and export targets under namespace `salias::`.

**Step 2: Generate package files**

Use `CMakePackageConfigHelpers` to configure and install package config and compatible version files under `${CMAKE_INSTALL_LIBDIR}/cmake/salias`.

**Step 3: Document downstream usage**

Add configure, install, `find_package`, and link examples to `README.md`.

### Task 5: Install consumer regression

**Files:**
- Create: `test/install/CMakeLists.txt`
- Create: `test/install/main.cpp`
- Create: `cmake/install_consumer_test.cmake`
- Modify: `CMakeLists.txt`

**Step 1: Add an independent consumer project**

Build a minimal executable that includes `<salias/salias.hpp>` and links only `salias::salias`.

**Step 2: Register an install test**

Install the current build into a temporary prefix, configure the consumer with `CMAKE_PREFIX_PATH`, and build it.

**Step 3: Run the test**

Expected: package discovery, include propagation, static dependency propagation, and linking all succeed.

### Task 6: Full verification

**Files:**
- Verify all modified files

**Step 1: Configure and build on macOS**

Run: `rtk cmake -S . -B /tmp/salias-macos-build -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DSALIAS_BUILD_BENCHMARKS=OFF`

Run: `rtk cmake --build /tmp/salias-macos-build -j 4`

**Step 2: Run tests outside restricted shm sandbox**

Run: `rtk ctest --test-dir /tmp/salias-macos-build --output-on-failure`

Expected: all discovered unit tests, examples, and install-consumer tests pass.

**Step 3: Verify direct installation**

Run: `rtk cmake --install /tmp/salias-macos-build --prefix /tmp/salias-install`

Expected: headers, libraries, target export, config, and version files are installed.

**Step 4: Review workspace**

Run: `rtk git diff --check && rtk git status --short`

Expected: no whitespace errors and only intended files are modified.

