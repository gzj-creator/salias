# Huge Page Support Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement explicit hugetlb-backed ring mappings and expose the huge page request on public `salias::Config`.

**Architecture:** `Mapping::create()` creates normal memfds for `HugePage::None` and hugetlb memfds for explicit 2 MiB or 1 GiB requests. In-process public channels translate `salias::HugePage` into `platform::HugePage`. Named hugetlbfs-backed IPC is covered by `2026-07-08-named-huge-ipc.md`.

**Tech Stack:** C++23, Linux `memfd_create`, `MFD_HUGETLB`, `MFD_HUGE_2MB`, `MFD_HUGE_1GB`, CMake, GTest, CTest.

---

### Task 1: Add Platform Huge Page Regression Tests

**Files:**
- Modify: `test/platform/mapping_test.cpp`

**Step 1: Write the failing tests**

Add tests that prove explicit huge page behavior without requiring configured huge pages:

```cpp
using salias::platform::HugePage;

TEST(MappingTest, HugePageRejectsSizeThatIsNotHugePageAligned) {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  ASSERT_GT(raw_page_size, 0);

  auto created = Mapping::create(MapOptions{
      .size = static_cast<std::size_t>(raw_page_size),
      .huge = HugePage::Size2MB,
  });

  ASSERT_FALSE(created);
  EXPECT_EQ(created.error(), PlatformError::InvalidSize);
}

TEST(MappingTest, HugePageCreateEitherMapsAliasesOrReportsUnavailable) {
  constexpr std::size_t kHuge2MiB = 2u * 1024u * 1024u;

  auto created = Mapping::create(MapOptions{
      .size = kHuge2MiB,
      .huge = HugePage::Size2MB,
  });

  if (!created) {
    EXPECT_EQ(created.error(), PlatformError::HugePageUnavailable)
        << "unexpected huge page error: " << describe(created.error());
    return;
  }

  Mapping mapping = std::move(created).value();
  ASSERT_NE(mapping.as_ptr(), nullptr);
  ASSERT_EQ(mapping.len(), kHuge2MiB);

  std::byte* const base = mapping.as_ptr();
  base[0] = std::byte{0x6A};
  EXPECT_EQ(byte_value(base[kHuge2MiB]), 0x6Au);
}
```

**Step 2: Run tests to verify RED**

Run:

```bash
rtk cmake --build build --target salias_platform_tests
rtk ./build/test/salias_platform_tests --gtest_filter='MappingTest.HugePage*'
```

Expected: the aligned huge page test fails because `Mapping::create()` still reports `HugePageUnavailable` unconditionally.

### Task 2: Implement L0 Huge Page Memfd Creation

**Files:**
- Modify: `src/core/platform/mapping.cpp`

**Step 1: Write minimal implementation**

Add helpers:

```cpp
constexpr std::size_t kHugePage2MiB = 2u * 1024u * 1024u;
constexpr std::size_t kHugePage1GiB = 1024u * 1024u * 1024u;

std::size_t huge_page_size(HugePage huge) noexcept;
int huge_memfd_flag(HugePage huge) noexcept;
bool is_valid_huge_size(std::size_t size, HugePage huge) noexcept;
int create_memfd(HugePage huge) noexcept;
```

Use `MFD_CLOEXEC` for normal mappings and `MFD_CLOEXEC | MFD_HUGETLB | huge_memfd_flag(huge)` for huge mappings. Map `memfd_create` or `ftruncate` failure on the huge path to `PlatformError::HugePageUnavailable`.

**Step 2: Run platform tests**

Run:

```bash
rtk cmake --build build --target salias_platform_tests
rtk ./build/test/salias_platform_tests --gtest_filter='MappingTest.*'
```

Expected: all mapping tests pass. On hosts without reserved huge pages, the aligned huge request reports `HugePageUnavailable`.

### Task 3: Expose HugePage on Public Config

**Files:**
- Modify: `src/salias/include/salias/config.hpp`
- Modify: `src/salias/src/channel.cpp`

**Step 1: Write the failing public API test**

Add a public named-channel rejection test to `test/api/channel_api_test.cpp`:

```cpp
TEST(ChannelApiTest, NamedCreateRejectsHugePageConfig) {
  salias::Config config = test_config();
  config.name = unique_name("named-huge");
  config.huge = salias::HugePage::Size2MB;

  auto channel = salias::Channel::create(config);

  EXPECT_FALSE(channel);
  EXPECT_EQ(channel.error(), salias::Error::BadConfig);
  unlink_named_channel(config.name);
}
```

Add an in-process compile/runtime test:

```cpp
TEST(ChannelApiTest, InProcessHugePageConfigReachesPlatformLayer) {
  salias::Config config = test_config();
  config.huge = salias::HugePage::Size2MB;
  config.capacity = 4096;

  auto channel = salias::Channel::create(config);

  EXPECT_FALSE(channel);
  EXPECT_EQ(channel.error(), salias::Error::BadConfig);
}
```

**Step 2: Run tests to verify RED**

Run:

```bash
rtk cmake --build build --target salias_api_tests
rtk ./build/test/salias_api_tests --gtest_filter='ChannelApiTest.*Huge*'
```

Expected: compile fails because `salias::HugePage` and `Config::huge` do not exist.

**Step 3: Implement public API translation**

Add public enum and config field:

```cpp
enum class HugePage { None, Size2MB, Size1GB };
```

Translate public enum to `platform::HugePage` in `channel.cpp`, include huge in `core_config()`, and reject named configs with huge before creating shared memory names.

**Step 4: Run API tests**

Run:

```bash
rtk cmake --build build --target salias_api_tests
rtk ./build/test/salias_api_tests --gtest_filter='ChannelApiTest.*Huge*'
```

Expected: the huge config tests pass.

### Task 4: Update Documentation

**Files:**
- Modify: `README.md`
- Modify: `doc/4-L0-platform.md`

**Step 1: Align docs with delivered behavior**

Update README to say explicit huge pages are supported for in-process anonymous mappings and public in-process channels, while named channels still reject huge page requests.

Update L0 platform docs to say huge page requests fail explicitly when unavailable; no automatic downgrade occurs in this implementation.

**Step 2: Run focused docs-adjacent tests**

Run:

```bash
rtk rg -n "huge|HugePage|大页" README.md doc/4-L0-platform.md src/salias/include/salias/config.hpp src/core/platform/mapping.cpp
```

Expected: docs and code describe the same support boundary.

### Task 5: Full Verification

**Files:**
- No new files.

**Step 1: Build**

Run:

```bash
rtk cmake --build build
```

Expected: build exits 0.

**Step 2: Test**

Run:

```bash
rtk ctest --test-dir build --output-on-failure
```

Expected: all registered tests pass.

**Step 3: Inspect diff**

Run:

```bash
rtk git diff --stat
rtk git diff -- src/core/platform/mapping.cpp src/salias/include/salias/config.hpp src/salias/src/channel.cpp test/platform/mapping_test.cpp test/api/channel_api_test.cpp README.md doc/4-L0-platform.md
```

Expected: diff is limited to huge page support, public config exposure, tests, and aligned docs.
