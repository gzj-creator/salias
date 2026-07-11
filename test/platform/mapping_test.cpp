/**
 * @file test/platform/mapping_test.cpp
 * @brief L0 平台层 magic-ring 双映射（mmap/memfd_create）的集成测试。
 * @details 本测试位于 L0 平台层之上，验证 Mapping 通过 mmap/memfd_create 构造的
 *  “首尾相接”双别名虚拟地址空间的核心不变式：两段相邻虚拟地址 [base, base+len) 与
 *  [base+len, base+2*len) 映射到同一物理后端，使写入 base[i] 等价于写入
 *  base[i+len]，从而让上层 L1 ring 无需做 sequence 低位环绕（wraparound）判断即可
 *  线性访问。测试覆盖三项关键语义：(1) 单进程内的跨尾别名写入可见性；(2) 通过 fork()
 *  验证子进程共享同一 memfd 后端（MAP_SHARED 语义）；(3) 显式 2 MiB huge page
 *  请求的容量对齐校验与可用/不可用两条路径。线程/进程模型：单进程内构造映射，
 *  fork() 后父子共享同一物理页。
 */
#include "core/platform/mapping.hpp"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstring>
#include <string_view>

namespace {
/// 匿名命名空间，存放 mapping 测试内部的辅助函数，限制链接范围到本编译单元。

using salias::platform::HugePage;
using salias::platform::MapOptions;
using salias::platform::Mapping;
using salias::platform::PlatformError;

/**
 * @brief 将 PlatformError 枚举转为稳定的字符串名称，便于断言失败时输出。
 * @param error 平台层错误码。
 * @return 对应枚举项的字符串视图；未知值返回 "Unknown"。
 * @note 用于 GTest 断言消息中可读地打印错误码，本身不涉及平台逻辑。
 */
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

/**
 * @brief 将 std::byte 转为无符号整数，便于断言中以数值形式比较字节内容。
 * @param value 待转换的字节值。
 * @return 该字节对应的 unsigned 整数值。
 */
// 将 byte 值转为无符号整数，便于断言阅读。
unsigned byte_value(std::byte value) noexcept { return std::to_integer<unsigned>(value); }

/**
 * @brief 验证双别名映射的跨尾写入可见性与 fork 后的物理页共享。
 * @details 构造一个单页大小的 Mapping（len 字节），利用双别名特性验证：
 *  写 base[0] 后 base[len] 可见同一字节（别名）；跨尾 memcpy 一段 8 字节模式，
 *  验证末尾 4 字节在前 4 字节镜像位置可见（环绕）。随后 fork() 子进程向共享内存
 *  写入新数据，父进程 waitpid 后验证子进程的写操作可见——这证明 MAP_SHARED +
 *  memfd 在父子进程间共享同一物理后端。
 */
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
  // 双别名核心断言：base[0] 与 base[len] 映射到同一物理字节。
  EXPECT_EQ(byte_value(base[len]), 0x5Au);

  base[len] = std::byte{0xA5};
  // 反向验证：写镜像段后原段可见同一字节。
  EXPECT_EQ(byte_value(base[0]), 0xA5u);

  constexpr std::array pattern{
      std::byte{0x10}, std::byte{0x11}, std::byte{0x12}, std::byte{0x13},
      std::byte{0x14}, std::byte{0x15}, std::byte{0x16}, std::byte{0x17},
  };
  // 在容量边界前 4 字节处写入 8 字节模式：前 4 字节落在段尾，后 4 字节越过边界。
  std::memcpy(base + len - 4, pattern.data(), pattern.size());

  for (std::size_t i = 0; i < pattern.size(); ++i) {
    EXPECT_EQ(byte_value(base[len - 4 + i]), byte_value(pattern[i])) << "at contiguous byte " << i;
  }
  // 越过边界的后 4 字节因双别名而环绕到段首，验证 magic-ring 的跨尾连续性。
  for (std::size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(byte_value(base[i]), byte_value(pattern[4 + i])) << "at wrapped byte " << i;
  }

  // fork() 后父子共享同一 memfd 物理后端（MAP_SHARED 语义），用于验证跨进程可见性。
  const pid_t child = ::fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    // 子进程：写共享内存后立即退出，写操作通过共享物理页对父进程可见。
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

  // 子进程在边界前 3 字节写入 child_pattern[0..5]：前 3 字节落段尾，
  // 后 3 字节环绕到段首，故 base[0..2] 对应 child_pattern[3..5]。
  EXPECT_EQ(byte_value(base[len]), 0xC3u);
  EXPECT_EQ(byte_value(base[0]), 0xC3u);
  EXPECT_EQ(byte_value(base[1]), 0xC4u);
  EXPECT_EQ(byte_value(base[2]), 0xC5u);
}

/**
 * @brief 验证显式 2 MiB huge page 请求在容量未按大页对齐时被拒绝。
 * @details 当请求 HugePage::Size2MB 但 size 仅为一个普通页（非 2 MiB 对齐）时，
 *  Mapping::create 应在容量校验阶段即返回 InvalidSize，而非落到运行时的
 *  HugePageUnavailable 资源错误。这确保大页对齐约束在前置校验中强制执行。
 */
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

/**
 * @brief 验证 2 MiB huge page 路径在环境可用时仍保持 magic-ring 双别名语义。
 * @details 以 2 MiB 对齐容量请求 Size2MB 大页。该测试具有环境敏感性：当运行环境
 *  提供大页时，验证双别名写入可见性（base[0] 与 base[len] 共享物理页）；当环境
 *  无可用大页时，接受 HugePageUnavailable 作为合法结果并跳过别名断言。两条路径
 *  任一通过即视为成功，保证测试在 CI 与受限环境均可运行。
 */
// 验证 2 MiB 大页路径在环境可用时仍保持 magic-ring 双别名语义。
TEST(MappingTest, HugePageCreateEitherMapsAliasesOrReportsUnavailable) {
  // 2 MiB huge page 的标准容量；必须按此粒度对齐才能成功申请大页。
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
