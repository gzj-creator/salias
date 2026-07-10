# MPSC and MPMC Example Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add runnable publisher/subscriber examples for MPSC and MPMC named IPC.

**Architecture:** The `example/` directory contains standalone CLI programs linked against the public `salias` target. Subscribers create or connect named channels and poll messages; publishers connect and publish text payloads through `try_claim/commit`. A CTest smoke script validates that the examples work as independent processes.

**Tech Stack:** C++23, CMake, CTest, public `salias/salias.hpp` API.

---

### Task 1: Smoke Test Harness

**Files:**
- Create: `example/example_smoke.sh`
- Modify: `CMakeLists.txt`
- Create: `example/CMakeLists.txt`

**Steps:**
1. Add `SALIAS_BUILD_EXAMPLES` and `add_subdirectory(example)`.
2. Add a CTest smoke test that references the four expected example target files.
3. Run CMake/build and confirm it fails before the example targets exist.

### Task 2: Example Programs

**Files:**
- Create: `example/common.hpp`
- Create: `example/mpsc_subscriber.cpp`
- Create: `example/mpsc_publisher.cpp`
- Create: `example/mpmc_subscriber.cpp`
- Create: `example/mpmc_publisher.cpp`
- Modify: `example/CMakeLists.txt`

**Steps:**
1. Implement shared CLI/error/connect helpers in `example/common.hpp`.
2. Implement MPSC owner subscriber and connecting publisher.
3. Implement MPMC subscriber with `--create` owner mode and connecting publisher.
4. Register all four targets in CMake.
5. Build examples and run `ctest -R salias_example_smoke`.
