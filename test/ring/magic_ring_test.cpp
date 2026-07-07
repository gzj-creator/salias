#include "core/platform/mapping.hpp"
#include "core/ring/cache_aligned.hpp"
#include "core/ring/magic_ring.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace {

using salias::platform::MapOptions;
using salias::platform::Mapping;
using salias::ring::CacheAligned;
using salias::ring::MagicRing;
using salias::ring::RingError;

// 将 byte 值转为无符号整数，便于断言阅读。
unsigned byte_value(std::byte value) noexcept {
  return std::to_integer<unsigned>(value);
}

// 验证双映射能让跨尾切片保持地址连续。
TEST(MagicRingTest, SlicesAreContiguousAcrossCapacityBoundary) {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  ASSERT_GT(raw_page_size, 0);

  auto mapping_result = Mapping::create(MapOptions{.size = static_cast<std::size_t>(raw_page_size)});
  ASSERT_TRUE(mapping_result);

  auto ring_result = MagicRing::create(std::move(mapping_result).value());
  ASSERT_TRUE(ring_result);

  MagicRing ring = std::move(ring_result).value();
  ASSERT_EQ(ring.capacity(), static_cast<std::size_t>(raw_page_size));
  ASSERT_EQ(ring.mask(), ring.capacity() - 1);

  constexpr std::array pattern{
      std::byte{0x20}, std::byte{0x21}, std::byte{0x22}, std::byte{0x23},
      std::byte{0x24}, std::byte{0x25}, std::byte{0x26}, std::byte{0x27},
      std::byte{0x28}, std::byte{0x29}, std::byte{0x2A}, std::byte{0x2B},
      std::byte{0x2C}, std::byte{0x2D}, std::byte{0x2E}, std::byte{0x2F},
  };

  auto writable = ring.slice_mut(ring.capacity() - 4, pattern.size());
  ASSERT_EQ(writable.size(), pattern.size());
  std::memcpy(writable.data(), pattern.data(), pattern.size());

  auto readable = ring.slice(ring.capacity() - 4, pattern.size());
  ASSERT_EQ(readable.size(), pattern.size());
  for (std::size_t i = 0; i < pattern.size(); ++i) {
    EXPECT_EQ(byte_value(readable[i]), byte_value(pattern[i])) << "at byte " << i;
  }

  auto wrapped_prefix = ring.slice(0, 12);
  for (std::size_t i = 0; i < wrapped_prefix.size(); ++i) {
    EXPECT_EQ(byte_value(wrapped_prefix[i]), byte_value(pattern[4 + i])) << "at prefix byte " << i;
  }
}

// 验证空映射不能创建 ring。
TEST(MagicRingTest, RejectsEmptyMovedFromMapping) {
  Mapping empty;

  auto ring_result = MagicRing::create(std::move(empty));

  ASSERT_FALSE(ring_result);
  EXPECT_EQ(ring_result.error(), RingError::ZeroLen);
}

// 验证容量检查基于单段逻辑 ring 大小。
TEST(MagicRingTest, ReportsFitAgainstSingleCapacity) {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  ASSERT_GT(raw_page_size, 0);

  auto mapping_result = Mapping::create(MapOptions{.size = static_cast<std::size_t>(raw_page_size)});
  ASSERT_TRUE(mapping_result);
  auto ring_result = MagicRing::create(std::move(mapping_result).value());
  ASSERT_TRUE(ring_result);

  MagicRing ring = std::move(ring_result).value();

  EXPECT_TRUE(ring.fits(ring.capacity()));
  EXPECT_FALSE(ring.fits(ring.capacity() + 1));
}

// 验证 cache-aligned 包装占用破坏性干扰大小的存储。
TEST(CacheAlignedTest, OccupiesAtLeastOneDestructiveInterferenceLine) {
  static_assert(alignof(CacheAligned<std::uint64_t>) >= salias::ring::kCacheLine);
  static_assert(sizeof(CacheAligned<std::uint64_t>) >= salias::ring::kCacheLine);
  static_assert(sizeof(CacheAligned<std::uint64_t>) % salias::ring::kCacheLine == 0);
}

}  // namespace
