# Final Independent-Process Performance Report Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build independent salias and Aeron publisher/subscriber benchmark programs, run the confirmed Tencent capacity/topology matrix, and replace historical comparison documents with one final report and README table.

**Architecture:** Worker executables never fork. A shell coordinator creates a per-run coordination directory, launches each role as a separate process, waits for ready files, publishes a start file, and aggregates machine-readable result files. salias workers use named channels; Aeron workers use independent clients plus the external media driver.

**Tech Stack:** C++23 salias workers, C++17 Aeron workers, CMake, Bash, Python 3 statistics, Tencent x86 Linux

---

### Task 1: Define the worker result protocol

**Files:**
- Create: `tools/final_bench/worker_common.hpp`
- Create: `test/tools/final_bench_worker_test.cpp`
- Modify: `test/CMakeLists.txt`

**Steps:**
1. Add failing tests for payload markers, coordination paths, result serialization, and percentile helpers.
2. Run the targeted CTest and confirm failure because the helper does not exist.
3. Implement fixed 64 B payload encoding, monotonic timestamps, ready/start waits, and key-value result writing.
4. Run the targeted test and confirm it passes.

### Task 2: Implement independent salias workers

**Files:**
- Create: `tools/final_bench/salias_publisher.cpp`
- Create: `tools/final_bench/salias_subscriber.cpp`
- Modify: `tools/final_bench/CMakeLists.txt`
- Modify: `CMakeLists.txt`

**Steps:**
1. Add CLI parsing for name, mode, role index, topology, capacity, batch, messages, coordination directory, and CPU.
2. Implement subscriber 0 channel creation and additional subscriber connection without worker forking.
3. Implement Publisher offer/offer_batch loops with BackPressured and maximum retry statistics.
4. Implement Subscriber polling, marker validation, release/flush behavior, and result output.
5. Build the two targets and run a local 2P1S and 2P2S smoke test through separate shell processes.

### Task 3: Implement independent Aeron workers

**Files:**
- Create: `tools/final_bench/aeron_publisher.cpp`
- Create: `tools/final_bench/aeron_subscriber.cpp`
- Create: `tools/final_bench/build_aeron_workers.sh`

**Steps:**
1. Reuse the worker protocol and CLI shape from salias.
2. Implement one Aeron client and ExclusivePublication per Publisher program.
3. Implement one Aeron client and Subscription per Subscriber program, waiting for all producer images.
4. Build against Aeron 1.52.0 and run a local independent-process MPSC smoke test with `aeronmd`.

### Task 4: Implement the final coordinator

**Files:**
- Create: `tools/final_bench/run_final_benchmark.sh`
- Create: `tools/final_bench/summarize.py`

**Steps:**
1. Launch workers as independent commands and synchronize through ready/start files.
2. Add 2P1S and 2P2S CPU layouts and Aeron driver lifecycle management.
3. Add the 1/4/64 MiB FIFO/Aeron and Ordered batch 1/8 matrices.
4. Interleave capacity/library order, execute warmups and measured rounds, and preserve all worker results.
5. Summarize median, p10-p90, latency, backpressure, and salias/Aeron ratios into CSV and Markdown tables.

### Task 5: Verify and deploy to Tencent

**Files:**
- Verify: `tools/final_bench/*`

**Steps:**
1. Run targeted tests and the Release build locally.
2. Sync the repository to an isolated Tencent directory.
3. Record machine configuration, build Release targets, and run smoke cases.
4. Run 3 warmups and 20 measured rounds for the full confirmed matrix.
5. Sync raw logs and generated summaries back to `doc/benchmarks/`.

### Task 6: Produce the single final report

**Files:**
- Create: `doc/performance-report.md`
- Modify: `README.md`
- Delete: historical comparison Markdown and benchmark logs under `doc/benchmarks/`
- Modify: `doc/13-optimization-retrospective.md`

**Steps:**
1. Write machine, software, compile, process, CPU allocation, command, fairness, and limitation sections.
2. Add final FIFO vs Aeron tables and salias-only Ordered tables.
3. Put the core final table directly in README and link the complete report.
4. Remove historical comparison reports/logs after final raw logs are safely stored.
5. Update the retrospective to reference the single final report.
6. Run link checks, `git diff --check`, and a final file inventory verification.
