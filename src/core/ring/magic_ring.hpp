/** @file src/core/ring/magic_ring.hpp
 * @brief 双映射 magic ring 环形缓冲句柄：基于 L0 连续双映射实现无环绕访问。
 * @details 本文件位于 L1 ring 环形缓冲层，是 salias 无锁 IPC 的地址连续性基础。
 *  L0 平台层（core/platform/mapping.hpp）通过 mmap/memfd_create 将同一份
 *  shared memory 以同一 memfd 映射到两段相邻虚拟地址 [base, base+2*cap)，使
 *  得任何长度不超过 cap 的访问在跨越逻辑容量边界时仍保持地址连续——即
 *  “magic ring”技巧。L1 在此之上只提供按 sequence 低位掩码索引的连续
 *  span（只读/可写），不建模位置所有权、发布顺序或内存序；这些由 L3 flow 层
 *  （生产/消费 position 与流控）负责。容量必须为 2 的幂，以支持 O(1) 位掩码
 *  环绕。线程/进程模型：多个 producer/consumer 经由各自上层协议共享同一映射，
 *  本类的成员函数本身不引入同步，调用者必须自行保证写入区间不重叠。
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

#include "core/platform/mapping.hpp"
#include "core/ring/error.hpp"

namespace salias::ring {
/// L1 ring 环形缓冲命名空间；提供基于双映射的 magic ring 及原子单元访问原语。

/**
 * @brief 双映射 magic ring 句柄，独占持有一份 L0 Mapping。
 * @details MagicRing 接管一份已校验（2 的幂、非零）的 Mapping，记其单段逻辑
 *  容量 cap_。由于底层是双映射，任意 pos 经 (pos & mask) 映射后，长度不超过
 *  cap_ 的访问在物理上地址连续，无需拆分处理环绕。所有权模型：独占映射，
 *  可移动不可拷贝（RAII）。线程安全：成员函数本身不同步；写入区间所有权由
 *  上层协议（L3 flow / L5 channel）保证。不变式：cap_ 为 0 或 2 的幂；
 *  cap_ == 0 时 base_ 可为 nullptr（空句柄）。
 */
class MagicRing {
 public:
  /// 创建结果类型：成功返回 MagicRing，失败返回 RingError。
  using CreateResult = std::expected<MagicRing, RingError>;

  /**
   * @brief 接管有效的 L0 双映射并校验其容量合法性。
   * @details 映射长度作为逻辑容量；第二段虚拟地址用于跨尾连续访问。校验指针
   *  非空、长度非零且为 2 的幂，任一不满足则返回对应 RingError。
   * @param mapping 已由 L0 成功创建的双映射；所有权转移到返回的 ring。
   * @retval MagicRing 校验通过的 ring 句柄。
   * @retval RingError 校验失败原因（ZeroLen / NotPowerOfTwo）。
   */
  // 接管有效的 L0 双映射；映射长度作为逻辑容量，第二段虚拟地址用于跨尾连续访问。
  static CreateResult create(platform::Mapping mapping) noexcept;

  /// 创建空 ring 句柄；base_ 为 nullptr、cap_ 为 0。
  // 创建空 ring 句柄。
  MagicRing() noexcept = default;
  /// 从另一个 ring 转移映射所有权（移动构造）。
  // 从另一个 ring 转移映射所有权。
  MagicRing(MagicRing&&) noexcept = default;
  /// 移动赋值映射所有权；释放当前映射后接管 other。
  // 移动赋值映射所有权。
  MagicRing& operator=(MagicRing&&) noexcept = default;
  /// 禁止拷贝；ring 独占映射所有权。
  // 禁止拷贝；ring 独占映射所有权。
  MagicRing(const MagicRing&) = delete;
  /// 禁止拷贝赋值；ring 独占映射所有权。
  // 禁止拷贝赋值；ring 独占映射所有权。
  MagicRing& operator=(const MagicRing&) = delete;

  /// @return 单段逻辑环形容量 cap_。
  // 返回单段逻辑环形容量。
  std::size_t capacity() const noexcept { return cap_; }
  /**
   * @brief 返回 2 的幂容量对应的索引掩码（cap_ - 1）。
   * @details 用于把任意 sequence/position 低位环绕到环形索引空间。
   *  空句柄（cap_==0）返回 0 以避免下溢。
   * @return 索引掩码；空 ring 返回 0。
   */
  // 返回 2 的幂容量对应的索引掩码；空 ring 返回 0。
  std::size_t mask() const noexcept { return cap_ == 0 ? 0 : cap_ - 1; }
  /// @param len 待放入长度。@return len 是否能放入单个逻辑环形容量。
  // 返回 len 是否能放入单个逻辑环形容量。
  bool fits(std::size_t len) const noexcept { return len <= cap_; }

  /**
   * @brief 从双映射 ring 返回连续只读 span。
   * @details 通过 pos & mask 将逻辑位置映射到环形索引，由于底层是双映射，
   *  长度不超过 cap_ 的访问在跨越逻辑边界时仍地址连续。
   * @param pos 逻辑位置（通常为上层 sequence 或 position）。
   * @param len 请求的连续长度；不得超过 capacity()。
   * @return 指向有效映射内存的 span；非法（空句柄或 len 超 cap_）返回空 span。
   * @note len 必须不超过 capacity()；调用者负责位置所有权和发布顺序。L1 只保证
   *  地址连续性。
   */
  // 约束：len 必须不超过 capacity()；调用者负责位置所有权和发布顺序。
  // L1 只保证地址连续性。
  std::span<const std::byte> slice(std::uint64_t pos, std::size_t len) const noexcept;
  /**
   * @brief 从双映射 ring 返回连续可写 span。
   * @details 地址连续性约束与只读 slice() 相同；写入所有权由上层协议保证。
   * @param pos 逻辑位置。@param len 请求的连续长度；不得超过 capacity()。
   * @return 指向有效映射内存的可写 span；非法返回空 span。
   * @note len 必须不超过 capacity()，且调用者必须拥有该逻辑写入区间。
   */
  // 约束：len 必须不超过 capacity()，且调用者必须拥有该逻辑写入区间。
  std::span<std::byte> slice_mut(std::uint64_t pos, std::size_t len) noexcept;

 private:
  /// 保存 create() 校验后的映射所有权、起始地址与容量。
  // 保存 create() 校验后的映射所有权、起始地址与容量。
  MagicRing(platform::Mapping mapping, std::byte* base, std::size_t cap) noexcept;

  std::byte* base_ = nullptr;        ///< 指向双映射起始虚拟地址（第一段）。
  std::size_t cap_ = 0;              ///< 单段逻辑容量；0 或 2 的幂。
  platform::Mapping mapping_;        ///< 持有的 L0 双映射与 fd 所有权（RAII）。
};

}  // namespace salias::ring
