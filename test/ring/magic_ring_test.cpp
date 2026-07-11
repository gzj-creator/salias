/**
 * @file test/ring/magic_ring_test.cpp
 * @brief L1 ring 层双映射 magic-ring 环形缓冲句柄的单元测试。
 * @details 本测试位于 L1 ring 环形缓冲层，验证 MagicRing 基于 L0 平台层 Mapping
 *  构造的双别名地址空间提供的核心语义：(1) 跨越逻辑容量边界的切片保持地址连续
 *  （magic-ring 技巧的核心收益——上层无需对 sequence 做低位环绕判断）；(2) 容量
 *  掩码正确性（cap 为 2 的幂，mask = cap-1）；(3) 空映射或非法映射被拒绝并返回
 *  对应 RingError；(4) fits() 容量检查基于单段逻辑 ring 大小而非双映射总长度。
 *  线程/进程模型：单进程单线程，写入区间由测试逻辑自身保证不重叠。
 */
#include "core/ring/magic_ring.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "core/platform/mapping.hpp"

namespace {
/// 匿名命名空间，存放 magic_ring 测试内部的辅助函数，限制链接范围到本编译单元。

using salias::platform::MapOptions;
using salias::platform::Mapping;
using salias::ring::MagicRing;
using salias::ring::RingError;

/**
 * @brief 将 std::byte 转为无符号整数，便于断言中以数值形式比较字节内容。
 * @param value 待转换的字节值。
 * @return 该字节对应的 unsigned 整数值。
 */
// 将 byte 值转为无符号整数，便于断言阅读。
unsigned byte_value(std::byte value) noexcept { return std::to_integer<unsigned>(value); }

/**
 * @brief 验证双映射使得跨容量边界的切片保持地址连续。
 * @details 构造单页大小的 Mapping 并以此创建 MagicRing，在容量边界前 4 字节处
 *  写入 16 字节模式：前 12 字节落段尾，后 4 字节越过边界。由于双别名，越过边界的
 *  部分环绕到段首。通过 slice() 验证连续区与环绕前缀的读取均符合写入模式，证明
 *  MagicRing 的 slice/slice_mut 无需调用者处理 wraparound。
 */
// 验证双映射能让跨尾切片保持地址连续。
TEST(MagicRingTest, SlicesAreContiguousAcrossCapacityBoundary) {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  ASSERT_GT(raw_page_size, 0);

  auto mapping_result =
      Mapping::create(MapOptions{.size = static_cast<std::size_t>(raw_page_size)});
  ASSERT_TRUE(mapping_result);

  auto ring_result = MagicRing::create(std::move(mapping_result).value());
  ASSERT_TRUE(ring_result);

  MagicRing ring = std::move(ring_result).value();
  ASSERT_EQ(ring.capacity(), static_cast<std::size_t>(raw_page_size));
  // 容量为 2 的幂时掩码 = cap-1，用于把 sequence 低位环绕到环形索引空间。
  ASSERT_EQ(ring.mask(), ring.capacity() - 1);

  // 16 字节测试模式，将写在容量边界前 4 字节处以触发跨尾环绕。
  constexpr std::array pattern{
      std::byte{0x20}, std::byte{0x21}, std::byte{0x22}, std::byte{0x23},
      std::byte{0x24}, std::byte{0x25}, std::byte{0x26}, std::byte{0x27},
      std::byte{0x28}, std::byte{0x29}, std::byte{0x2A}, std::byte{0x2B},
      std::byte{0x2C}, std::byte{0x2D}, std::byte{0x2E}, std::byte{0x2F},
  };

  // 从容量边界前 4 字节处申请 16 字节可写切片：前 12 字节落段尾，后 4 字节越过边界。
  auto writable = ring.slice_mut(ring.capacity() - 4, pattern.size());
  ASSERT_EQ(writable.size(), pattern.size());
  std::memcpy(writable.data(), pattern.data(), pattern.size());

  // 只读切片从同一起点读取 16 字节，验证写入内容在连续区中完整可见。
  auto readable = ring.slice(ring.capacity() - 4, pattern.size());
  ASSERT_EQ(readable.size(), pattern.size());
  for (std::size_t i = 0; i < pattern.size(); ++i) {
    EXPECT_EQ(byte_value(readable[i]), byte_value(pattern[i])) << "at byte " << i;
  }

  // 从段首读取 12 字节：因双别名，pattern 越过边界的后 12 字节环绕到此处。
  // pattern 从偏移 4 起（即越过容量边界 4 字节后的部分）应与此切片匹配。
  auto wrapped_prefix = ring.slice(0, 12);
  for (std::size_t i = 0; i < wrapped_prefix.size(); ++i) {
    EXPECT_EQ(byte_value(wrapped_prefix[i]), byte_value(pattern[4 + i])) << "at prefix byte " << i;
  }
}

/**
 * @brief 验证传入空映射（move 后）时 MagicRing::create 返回 RingError::ZeroLen。
 * @details 空 Mapping 的 base 为 nullptr、len 为 0，不满足 2 的幂非零容量校验，
 *  create 必须拒绝并返回 ZeroLen，避免上层在空地址上产生越界访问。
 */
// 验证空映射不能创建 ring。
TEST(MagicRingTest, RejectsEmptyMovedFromMapping) {
  // 默认构造的空 Mapping，不持有任何虚拟映射资源。
  Mapping empty;

  auto ring_result = MagicRing::create(std::move(empty));

  ASSERT_FALSE(ring_result);
  EXPECT_EQ(ring_result.error(), RingError::ZeroLen);
}

/**
 * @brief 验证 fits() 容量检查基于单段逻辑 ring 大小，而非双映射总长度。
 * @details MagicRing 底层虽占 2*cap 虚拟地址，但单次 slice 长度不得超过单段逻辑
 *  容量 cap。因此 fits(cap) 应为 true，fits(cap+1) 应为 false。
 */
// 验证容量检查基于单段逻辑 ring 大小。
TEST(MagicRingTest, ReportsFitAgainstSingleCapacity) {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  ASSERT_GT(raw_page_size, 0);

  auto mapping_result =
      Mapping::create(MapOptions{.size = static_cast<std::size_t>(raw_page_size)});
  ASSERT_TRUE(mapping_result);
  auto ring_result = MagicRing::create(std::move(mapping_result).value());
  ASSERT_TRUE(ring_result);

  MagicRing ring = std::move(ring_result).value();

  EXPECT_TRUE(ring.fits(ring.capacity()));
  EXPECT_FALSE(ring.fits(ring.capacity() + 1));
}

// 验证 cache-aligned 包装占用破坏性干扰大小的存储。
}  // namespace
