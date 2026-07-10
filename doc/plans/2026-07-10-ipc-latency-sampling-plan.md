# IPC Sampled Latency Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add matched sampled one-way p50/p99 latency reporting to the salias and Aeron multi-process IPC comparison tools.

**Architecture:** Both tools share a header-only logarithmic histogram and timestamp helpers. Each consumer owns one shared-memory histogram row, while the parent merges rows after all children exit and prints per-consumer plus aggregate percentiles.

**Tech Stack:** C++17-compatible helper code, C++23 salias tool, Aeron C++ 1.52.0, GTest, CMake/Ninja, Linux `clock_gettime` and shared anonymous `mmap`.

---

### Task 1: Histogram math

**Files:**
- Create: `tools/aeron_compare/latency_histogram.hpp`
- Create: `test/tools/latency_histogram_test.cpp`
- Modify: `test/CMakeLists.txt`

1. Write failing tests for exact sub-64ns buckets, power-of-two boundaries, p50/p99 rank selection, and merged counts.
2. Run `cmake --build build/colima-release --target salias_latency_histogram_tests` and confirm the target fails before the helper exists.
3. Implement the minimal C++17-compatible fixed-size logarithmic histogram helpers.
4. Build and run `build/colima-release/test/salias_latency_histogram_tests`.

### Task 2: salias latency sampling

**Files:**
- Modify: `tools/aeron_compare/salias_ipc_compare.cpp`

1. Add a failing CLI smoke expectation for `--latency-sample-rate` output fields.
2. Extend options validation, dynamic shared mapping size, producer timestamp writes, consumer histogram recording, and percentile output.
3. Run the smoke with 1P/1C and confirm non-zero sample count plus ordered percentile fields.
4. Run FIFO and ordered 2P/2C smoke cases.

### Task 3: Aeron latency sampling

**Files:**
- Modify: `tools/aeron_compare/aeron_ipc_compare.cpp`
- Modify: `tools/aeron_compare/run_release_compare.sh`

1. Add the same CLI option and payload validation to the Aeron tool.
2. Reuse the shared histogram, timestamp sampled `tryClaim` messages, and record in each subscription handler.
3. Pass `LATENCY_SAMPLE_RATE` through the release comparison script and select Aeron `mpmc` whenever consumers exceed one.
4. Build Aeron and run a 1P/1C then 2P/2C latency smoke.

### Task 4: Verification and Tencent run

**Files:**
- Modify: `doc/benchmarks/tencent-aeron-comparison-2026-07-10.md`
- Modify: `doc/plans/2026-07-10-dual-engine-architecture.md`

1. Run clang-format and `git diff --check`.
2. Run ASan/UBSan, TSan, and Release test suites in Colima.
3. Sync the workspace to Tencent and rebuild salias/Aeron.
4. Run 2P/2C fixed-budget and scheduler-balanced latency comparisons with sample rate 64, warmups, and repeated measured rounds.
5. Run the higher-process MPMC diagnostic matrix if the four-core runtime remains stable.
6. Archive raw logs, checksums, p50/p99 summaries, and hardware limitations.
