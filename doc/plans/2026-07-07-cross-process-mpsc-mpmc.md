# Cross-Process MPSC/MPMC Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add named cross-process MPSC first, then named MPMC fanout, so salias can benchmark real IPC against Aeron IPC.

**Architecture:** Reuse the existing shared-memory ring and control block handshake. Add shared channel wrappers that store positions in the named control block instead of in-process cache-aligned fields. MPMC fanout uses Aeron-style pub/sub semantics: every subscriber receives every publisher's messages.

**Tech Stack:** C++23, POSIX shared memory, Linux `mmap`/`futex`, CMake/Ninja, GTest, Colima Linux aarch64.

---

### Task 1: Named MPSC Failing Test

**Files:**
- Modify: `test/api/channel_api_test.cpp`

**Step 1: Write the failing test**

Add a fork-based test where the owner creates `Config{name, Mode::Mpsc}`, two
child processes connect as publishers, and the owner receives both messages from
one subscriber.

**Step 2: Verify RED**

Run:

```bash
colima ssh -- cmake --build --preset release --target salias_api_tests
colima ssh -- ./build/release/test/salias_api_tests --gtest_filter='ChannelApiTest.NamedMpsc*'
```

Expected: fails because named MPSC currently returns `BadConfig`.

### Task 2: Shared MPSC Core Wrapper

**Files:**
- Create: `src/core/channel/shared_mpsc.hpp`
- Modify: `src/core/CMakeLists.txt` only if needed

**Step 1: Implement shared MPSC wrapper**

Mirror `MpscChannel` but take external pointers for reserved tail, consumer
position, and wait word. Keep the same claim/commit/read gap behavior.

**Step 2: Compile**

Run:

```bash
colima ssh -- cmake --build --preset release --target salias_core
```

Expected: core builds.

### Task 3: Named Mode Dispatch

**Files:**
- Modify: `src/salias/src/channel.cpp`
- Modify: `src/salias/include/salias/config.hpp`
- Modify: `README.md`

**Step 1: Generalize named state**

Keep the existing named control layout but interpret `producer_pos` as MPSC
`reserved_tail` when `mode == Mode::Mpsc`.

**Step 2: Create named MPSC**

Allow `Channel::create(Config{name, Mode::Mpsc})` and store `mode` metadata.

**Step 3: Connect by metadata mode**

Change `Channel::connect(name)` to inspect `control->mode` and build either
shared SPSC or shared MPSC.

**Step 4: Verify GREEN**

Run the named MPSC focused test. Then run release CTest.

### Task 4: Named MPMC Fanout Design Checkpoint

**Files:**
- Modify: `doc/plans/2026-07-07-cross-process-mpsc-mpmc-design.md`

**Step 1: Update from MPSC implementation findings**

Record any ABI or lifecycle constraints found during MPSC.

**Step 2: Confirm fanout semantics**

MPMC means every subscriber receives every message from every publisher. It is
not a competing-consumer queue.

### Task 5: Named MPMC Fanout Implementation

**Files:**
- Create: `src/core/channel/shared_mpmc.hpp`
- Modify: `src/salias/src/channel.cpp`
- Modify: `src/salias/include/salias/config.hpp`
- Modify: `test/api/channel_api_test.cpp`

**Step 1: Write the failing test**

Add a fork-based test where the owner creates `Config{name, Mode::Mpmc}`, two
subscriber child processes connect and subscribe, two publisher child processes
connect and publish one message each, and every subscriber receives both
publisher messages.

**Step 2: Implement shared MPMC**

Use one shared `reserved_tail`, a fixed subscriber table, one head per
subscriber, and producer backpressure based on the minimum active subscriber
head.

**Step 3: Verify GREEN**

Run:

```bash
colima ssh -- cmake --build --preset release --target salias_api_tests
colima ssh -- ./build/release/test/salias_api_tests --gtest_filter='ChannelApiTest.NamedMpmc*'
```

Expected: named MPMC fanout test passes.

### Task 6: Cross-Process Benchmark

**Files:**
- Modify: `tools/aeron_compare/run_release_compare.sh`
- Modify or create: `tools/aeron_compare/salias_ipc_compare.cpp`

**Step 1: Implement salias multi-process comparison**

Use actual forked producer/subscriber processes for salias named modes.

**Step 2: Run warmup plus 5 measured rounds**

Compare against Aeron IPC with matching producer/subscriber counts, payload, and
poll/fragment limits.

**Status:** Implemented. `tools/aeron_compare/salias_ipc_compare.cpp` runs
forked salias publisher/subscriber processes over named SPSC-compatible shared
memory control/ring objects for MPSC and MPMC. `tools/aeron_compare/aeron_ipc_compare.cpp`
also runs forked Aeron producer/subscriber processes against an external
`aeronmd` IPC media driver. The default release script runs MPSC `4P/1C` and
MPMC `4P/2C` with one warmup round and five measured rounds.
