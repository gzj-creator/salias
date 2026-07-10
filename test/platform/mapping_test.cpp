#include "core/platform/mapping.hpp"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstring>
#include <string_view>

namespace {

using salias::platform::HugePage;
using salias::platform::MapOptions;
using salias::platform::Mapping;
using salias::platform::PlatformError;

// 返回用于平台错误断言的稳定名称。
std::string_view describe(PlatformError error) noexcept {
  switch (error) {
    case PlatformError::Ok:
      return "Ok";
    case PlatformError::MemfdCreateFailed:
      return "MemfdCreateFailed";
    case PlatformError::FtruncateFailed:
      return "FtruncateFailed";
    case PlatformError::ReserveFailed:
      return "ReserveFailed";
    case PlatformError::MapFixedFailed:
      return "MapFixedFailed";
    case PlatformError::UnmapFailed:
      return "UnmapFailed";
    case PlatformError::HugePageUnavailable:
      return "HugePageUnavailable";
    case PlatformError::NumaUnavailable:
      return "NumaUnavailable";
    case PlatformError::InvalidSize:
      return "InvalidSize";
  }
  return "Unknown";
}

// 将 byte 值转为无符号整数，便于断言阅读。
unsigned byte_value(std::byte value) noexcept { return std::to_integer<unsigned>(value); }

// 验证跨尾写入和 fork 进程中的别名映射行为。
TEST(MappingTest, ForkSharesAliasedPagesAndCrossBoundaryWrites) {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  ASSERT_GT(raw_page_size, 0);

  const auto len = static_cast<std::size_t>(raw_page_size);
  auto created = Mapping::create(MapOptions{.size = len});
  ASSERT_TRUE(created) << "Mapping::create failed: " << describe(created.error());

  Mapping mapping = std::move(created).value();
  ASSERT_NE(mapping.as_ptr(), nullptr);
  ASSERT_EQ(mapping.len(), len);

  std::byte* const base = mapping.as_ptr();

  base[0] = std::byte{0x5A};
  EXPECT_EQ(byte_value(base[len]), 0x5Au);

  base[len] = std::byte{0xA5};
  EXPECT_EQ(byte_value(base[0]), 0xA5u);

  constexpr std::array pattern{
      std::byte{0x10}, std::byte{0x11}, std::byte{0x12}, std::byte{0x13},
      std::byte{0x14}, std::byte{0x15}, std::byte{0x16}, std::byte{0x17},
  };
  std::memcpy(base + len - 4, pattern.data(), pattern.size());

  for (std::size_t i = 0; i < pattern.size(); ++i) {
    EXPECT_EQ(byte_value(base[len - 4 + i]), byte_value(pattern[i])) << "at contiguous byte " << i;
  }
  for (std::size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(byte_value(base[i]), byte_value(pattern[4 + i])) << "at wrapped byte " << i;
  }

  const pid_t child = ::fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    base[0] = std::byte{0x7B};
    constexpr std::array child_pattern{
        std::byte{0xC0}, std::byte{0xC1}, std::byte{0xC2},
        std::byte{0xC3}, std::byte{0xC4}, std::byte{0xC5},
    };
    std::memcpy(base + len - 3, child_pattern.data(), child_pattern.size());
    _exit(0);
  }

  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);

  EXPECT_EQ(byte_value(base[len]), 0xC3u);
  EXPECT_EQ(byte_value(base[0]), 0xC3u);
  EXPECT_EQ(byte_value(base[1]), 0xC4u);
  EXPECT_EQ(byte_value(base[2]), 0xC5u);
}

// 验证显式 2 MiB 大页请求会先做大页粒度校验，而不是落到运行时资源错误。
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

// 验证 2 MiB 大页路径在环境可用时仍保持 magic-ring 双别名语义。
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

}  // namespace
