# Named Huge Page IPC Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Support explicit huge pages for public named MPSC/MPMC IPC and extend the release comparison tooling to benchmark normal, 2 MiB, and 1 GiB page backends before comparing against Aeron IPC.

**Architecture:** Named control blocks remain POSIX shm. Named ring data objects keep using POSIX shm for normal pages and use hugetlbfs files for huge pages, with backend and huge page metadata stored in the control flags. Benchmarks add a `--page normal|huge2m|huge1g` selector and print page metadata in machine-readable result rows.

**Tech Stack:** C++23, Linux POSIX shm, hugetlbfs files, `mmap`, `std::expected`, CMake/Ninja, GTest, shell benchmark runner, Aeron C++ IPC compare tool.

---

### Task 1: Public Named Huge Validation Tests

**Files:**
- Modify: `test/api/channel_api_test.cpp`

**Step 1: Write failing tests**

Add tests:

```cpp
TEST(ChannelApiTest, NamedHugeRejectsCapacityThatIsNotHugePageAligned) {
  salias::Config config = test_config();
  config.name = unique_name("named-huge-bad-capacity");
  config.mode = salias::Mode::Mpsc;
  config.huge = salias::HugePage::Size2MB;

  auto channel = salias::Channel::create(config);

  ASSERT_FALSE(channel);
  EXPECT_EQ(channel.error(), salias::Error::BadConfig);
  unlink_control_shm(config.name);
}

TEST(ChannelApiTest, NamedHugeReportsPlatformFailWhenHugetlbfsDirectoryIsMissing) {
  ScopedEnv env("SALIAS_HUGETLBFS_DIR", "/tmp/salias-missing-hugetlbfs-dir");
  salias::Config config = test_config();
  config.name = unique_name("named-huge-missing-dir");
  config.mode = salias::Mode::Mpsc;
  config.capacity = 2u * 1024u * 1024u;
  config.huge = salias::HugePage::Size2MB;

  auto channel = salias::Channel::create(config);

  ASSERT_FALSE(channel);
  EXPECT_EQ(channel.error(), salias::Error::PlatformFail);
  unlink_control_shm(config.name);
}
```

Add a small `ScopedEnv` helper in the test file to set and restore environment variables.

**Step 2: Verify RED**

Run:

```bash
rtk docker run --rm --entrypoint sh -v "$PWD":/work:ro -w /work kivy/buildozer:latest -lc 'apt-get update >/tmp/apt.log && apt-get install -y ninja-build libgtest-dev >/tmp/apt-install.log && cmake -S . -B /tmp/salias-build -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DSALIAS_BUILD_BENCHMARKS=OFF && cmake --build /tmp/salias-build --target salias_api_tests && /tmp/salias-build/test/salias_api_tests --gtest_filter="ChannelApiTest.NamedHuge*"'
```

Expected: tests fail because named huge currently returns `BadConfig` before trying any backend.

### Task 2: Named Ring Backend Selection

**Files:**
- Modify: `src/salias/src/channel.cpp`

**Step 1: Implement minimal backend metadata and helpers**

Add helpers in the anonymous namespace:

```cpp
enum class NamedRingBackend { PosixShm, Hugetlbfs };
struct NamedRingSpec { NamedRingBackend backend; HugePage huge; };

std::uint32_t named_flags_for(HugePage huge) noexcept;
std::optional<NamedRingSpec> named_ring_spec_from_flags(std::uint32_t flags) noexcept;
std::string hugetlbfs_dir();
std::string huge_ring_path(std::string_view name);
int create_sized_hugetlbfs_file(std::string_view name, std::size_t size) noexcept;
int open_hugetlbfs_file(std::string_view name) noexcept;
void unlink_ring_backend(std::string_view shm_name, std::string_view public_name,
                         NamedRingSpec spec) noexcept;
Result<ring::MagicRing> map_ring_from_fd(int fd, std::size_t capacity, HugePage huge) noexcept;
```

Use `SALIAS_HUGETLBFS_DIR` if set, otherwise `/dev/hugepages`.

**Step 2: Store flags and create/open the right ring backend**

Update named SPSC/MPSC/MPMC create functions to:

- reject invalid huge capacity through existing public config validation
- create POSIX shm ring for `HugePage::None`
- create hugetlbfs ring file for huge pages
- store `control->flags = named_flags_for(config.huge)`

Update connect functions to:

- parse `control->flags`
- reject unknown flags with `BadConfig`
- open POSIX shm or hugetlbfs ring accordingly
- pass the parsed huge page into `map_ring_from_fd`

**Step 3: Verify GREEN**

Run the focused API tests from Task 1.

Expected: the bad capacity test returns `BadConfig`; the missing directory test returns `PlatformFail`.

### Task 3: Named Huge Connect Regression Tests

**Files:**
- Modify: `test/api/channel_api_test.cpp`

**Step 1: Write failing metadata validation test**

Add a synthetic control block test that sets unsupported huge flags and verifies connect rejects it:

```cpp
TEST(ChannelApiTest, NamedConnectRejectsUnknownRingBackendFlags) {
  const std::string name = unique_name("bad-ring-flags");
  TestNamedSpscControl control{};
  control.magic = kTestNamedMagic;
  control.version = kTestNamedVersion;
  control.mode = static_cast<std::uint32_t>(salias::Mode::Spsc);
  control.flags = 0xFFFFu;
  control.capacity = test_config().capacity;
  control.ready = 1;
  write_control_shm(name, control);

  auto connected = salias::Channel::connect(name);

  EXPECT_FALSE(connected);
  EXPECT_EQ(connected.error(), salias::Error::BadConfig);
  unlink_control_shm(name);
}
```

**Step 2: Verify RED/GREEN**

Run the focused test. If Task 2 already validates flags, this test may pass immediately; if so, keep it as regression coverage and document that the implementation preceded this regression test because the flag parser was created in Task 2.

### Task 4: Extend salias IPC Compare Page Option

**Files:**
- Modify: `tools/aeron_compare/salias_ipc_compare.cpp`

**Step 1: Write failing behavior**

Update the tool so `Options` has `std::string page = "normal"` and `make_named_config()` maps:

- `normal` -> `salias::HugePage::None`
- `huge2m` -> `salias::HugePage::Size2MB`
- `huge1g` -> `salias::HugePage::Size1GB`

Add `--page` parsing and include `page=<value>` in `RESULT`.

**Step 2: Verify RED**

Build only `salias_ipc_compare` and run:

```bash
./build/release/bench/salias_ipc_compare --scenario mpsc --messages 100 --page normal
```

Expected before implementation: unknown argument `--page`.

**Step 3: Implement**

Add parser, config mapping, usage text, and result field.

**Step 4: Verify GREEN**

Run a short normal-page MPSC and MPMC benchmark in Linux:

```bash
/tmp/salias-build/bench/salias_ipc_compare --scenario mpsc --messages 1000 --page normal
/tmp/salias-build/bench/salias_ipc_compare --scenario mpmc --messages 1000 --page normal
```

Expected: each prints one `RESULT library=salias-ipc ... page=normal ...` row.

### Task 5: Extend Release Compare Script

**Files:**
- Modify: `tools/aeron_compare/run_release_compare.sh`

**Step 1: Add page matrix**

Add environment variable:

```bash
SALIAS_PAGES="${SALIAS_PAGES:-normal huge2m huge1g}"
```

Run salias for each page before the Aeron run. If a huge run exits non-zero, print:

```text
SKIP library=salias-ipc scenario=<scenario> page=<page> reason=<status>
```

Keep Aeron as a single IPC baseline per scenario per measured round.

**Step 2: Verify script syntax**

Run:

```bash
rtk bash -n tools/aeron_compare/run_release_compare.sh
```

Expected: no syntax errors.

### Task 6: Documentation

**Files:**
- Modify: `README.md`
- Modify: `doc/benchmarks/aeron-baseline.md`

**Step 1: Update docs**

README should say named channels support `Config::huge` when hugetlbfs is available, defaulting to `/dev/hugepages` and overrideable by `SALIAS_HUGETLBFS_DIR`.

Benchmark docs should describe the new `page=` result field and note that huge page rows can be skipped when the system lacks configured huge pages.

**Step 2: Verify docs**

Run:

```bash
rtk rg -n "SALIAS_HUGETLBFS_DIR|--page|page=|huge2m|huge1g" README.md doc/benchmarks/aeron-baseline.md tools/aeron_compare
```

Expected: docs and tools mention the same page options.

### Task 7: Full Verification

**Files:**
- No new files.

**Step 1: Build and test in Linux**

Run:

```bash
rtk docker run --rm --entrypoint sh -v "$PWD":/work:ro -w /work kivy/buildozer:latest -lc 'apt-get update >/tmp/apt.log && apt-get install -y ninja-build libgtest-dev >/tmp/apt-install.log && cmake -S . -B /tmp/salias-build -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DSALIAS_BUILD_BENCHMARKS=OFF && cmake --build /tmp/salias-build && ctest --test-dir /tmp/salias-build --output-on-failure'
```

Expected: all tests pass.

**Step 2: Build benchmark target**

Run:

```bash
rtk docker run --rm --entrypoint sh -v "$PWD":/work:ro -w /work kivy/buildozer:latest -lc 'apt-get update >/tmp/apt.log && apt-get install -y ninja-build libgtest-dev libbenchmark-dev >/tmp/apt-install.log && cmake -S . -B /tmp/salias-build -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DSALIAS_BUILD_BENCHMARKS=ON -DCMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE=OFF && cmake --build /tmp/salias-build --target salias_ipc_compare && /tmp/salias-build/bench/salias_ipc_compare --scenario mpsc --messages 1000 --page normal && /tmp/salias-build/bench/salias_ipc_compare --scenario mpmc --messages 1000 --page normal'
```

Expected: benchmark target builds and both short normal-page runs print `RESULT`.

**Step 3: Static checks**

Run:

```bash
rtk git diff --check
rtk bash -n tools/aeron_compare/run_release_compare.sh
```

Expected: no whitespace or shell syntax errors.

