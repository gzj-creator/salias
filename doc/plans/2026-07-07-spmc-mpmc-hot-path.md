# SPMC/MPMC Hot Path Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add comparable public hot paths for salias SPMC/MPMC benchmarking, starting with batch consumer polling.

**Architecture:** Add a public non-owning callback poll API on `Subscriber` that visits the channel variant once and drains up to a caller-specified message limit. Then add public producer claim/commit and update comparison benchmarks to select matched salias/Aeron hot paths.

**Tech Stack:** C++23, CMake/Ninja, GTest, google-benchmark, Colima Linux aarch64 for local verification.

---

### Task 1: Public `Subscriber::poll`

**Files:**
- Modify: `src/salias/include/salias/subscriber.hpp`
- Modify: `src/salias/src/channel.cpp`
- Test: `test/api/channel_api_test.cpp`

**Step 1: Write the failing tests**

Add tests that call the desired public API before it exists:

```cpp
TEST(ChannelApiTest, PollDrainsAvailableMessagesAndAutoReleasesThem) {
  auto channel_result = salias::Channel::create(test_config());
  ASSERT_TRUE(channel_result);
  auto channel = std::move(channel_result).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array first{std::byte{0x11}};
  std::array second{std::byte{0x22}};
  ASSERT_TRUE(publisher.offer(first));
  ASSERT_TRUE(publisher.offer(second));

  std::vector<unsigned> seen;
  const auto count = subscriber.poll(64, [&](const salias::Message& message) noexcept {
    seen.push_back(std::to_integer<unsigned>(message.payload.front()));
  });

  EXPECT_EQ(count, 2u);
  EXPECT_THAT(seen, ::testing::ElementsAre(0x11u, 0x22u));
  EXPECT_FALSE(subscriber.try_recv().has_value());
}
```

Also add a zero-limit case:

```cpp
TEST(ChannelApiTest, PollWithZeroLimitDoesNotCallHandler) {
  auto channel_result = salias::Channel::create(test_config());
  ASSERT_TRUE(channel_result);
  auto channel = std::move(channel_result).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array payload{std::byte{0x33}};
  ASSERT_TRUE(publisher.offer(payload));

  bool called = false;
  EXPECT_EQ(subscriber.poll(0, [&](const salias::Message&) noexcept { called = true; }), 0u);
  EXPECT_FALSE(called);
  EXPECT_TRUE(subscriber.try_recv().has_value());
}
```

**Step 2: Run the focused test to verify RED**

Run:

```bash
colima ssh -- cmake --build --preset release --target salias_api_tests
```

Expected: compile failure because `Subscriber::poll` is not declared.

**Step 3: Add public API declaration**

In `subscriber.hpp`, add:

```cpp
using PollCallback = void (*)(const Message& message, void* user) noexcept;

std::size_t poll(std::uint32_t max_messages, PollCallback callback, void* user) noexcept;

template <class Handler>
std::size_t poll(std::uint32_t max_messages, Handler& handler) noexcept {
  static_assert(std::is_nothrow_invocable_v<Handler&, const Message&>);
  return poll(max_messages,
              [](const Message& message, void* user) noexcept {
                (*static_cast<Handler*>(user))(message);
              },
              &handler);
}
```

**Step 4: Implement the minimal loop**

In `channel.cpp`, implement `Subscriber::poll` by visiting `state_->channel` once,
creating the matching core `rx` once, and looping until either `max_messages` is
reached or `try_recv()` returns no message. Convert each `flow::Message` to
public `Message`, call the callback, then release the original flow message.

**Step 5: Verify GREEN**

Run:

```bash
colima ssh -- cmake --build --preset release --target salias_api_tests
colima ssh -- ./build/release/test/salias_api_tests --gtest_filter='ChannelApiTest.Poll*'
```

Expected: both poll tests pass.

**Step 6: Run the wider suite**

Run:

```bash
colima ssh -- cmake --build --preset release
colima ssh -- ctest --test-dir build/release --output-on-failure
```

Expected: all tests pass.

### Task 2: Use Batch Poll in salias Comparison Benchmark

**Files:**
- Modify: `bench/channel_compare.cpp`

**Step 1: Add a benchmark flag**

Add `--poll-limit N`, defaulting to `64`. In salias consumer loops, call
`subscriber.poll(options.poll_limit, handler)` and yield only when it returns 0.

**Step 2: Verify benchmark builds**

Run:

```bash
colima ssh -- cmake --build --preset release --target salias_bench_compare
```

Expected: build succeeds.

**Step 3: Run a smoke comparison**

Run:

```bash
colima ssh -- ./build/release/bench/salias_bench_compare --scenario spmc --messages 10000 --producers 1 --consumers 2 --payload 64 --poll-limit 64
```

Expected: one `RESULT library=salias scenario=spmc ...` line and no delivery mismatch.

### Task 3: Public Producer Claim/Commit

**Files:**
- Modify: `src/salias/include/salias/publisher.hpp`
- Modify: `src/salias/src/channel.cpp`
- Test: `test/api/channel_api_test.cpp`

**Step 1: Write failing tests**

Add a test that claims a payload span, writes bytes into it, commits, and observes
the same payload from a subscriber.

**Step 2: Implement a move-only claim wrapper**

Add a public claim type that owns a reserved `flow::Claim` plus the channel state.
Expose `payload()` and `commit()`.

**Step 3: Verify**

Run focused API tests, then full release CTest.

### Task 4: Benchmark `offer|claim`

**Files:**
- Modify: `bench/channel_compare.cpp`
- Modify if needed: `tools/aeron_compare/run_release_compare.sh`

**Step 1: Add `--publish-api offer|claim`**

Use `claim` by default for strict salias/Aeron comparison and keep `offer` for
backward-compatible measurement.

**Step 2: Run 5-round comparison**

Run warmup plus 5 measured release comparisons in Colima Linux, with matching
poll/fragment limits.

### Task 5: Native MPMC Design Follow-Up

**Files:**
- Create: `doc/plans/YYYY-MM-DD-native-mpmc-fanout-design.md`

**Step 1: Design only**

Do not implement native MPMC until batch poll and claim/commit benchmark deltas
are measured. Use those measurements to choose the native topology.
