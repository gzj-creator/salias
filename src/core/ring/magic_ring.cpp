/** @file src/core/ring/magic_ring.cpp
 * @brief MagicRing 双映射环形缓冲的实现。
 * @details 本文件位于 L1 ring 环形缓冲层，实现 magic_ring.hpp 中声明的工厂方法与
 *  slice 访问器。核心思路：L0 平台层已将同一份 shared memory 经 mmap/memfd_create
 *  双映射到相邻虚拟地址区间，使得任意长度不超过容量的访问在跨越逻辑容量边界时
 *  仍地址连续。本文件负责校验容量（2 的幂、非零）并据此构造 ring，提供基于
 *  sequence 低位掩码 (pos & mask) 的连续 span 切片；位置所有权、发布顺序与内存序
 *  由上层 L3 flow / L5 channel 协议保证，本层只做地址连续性保证。线程/进程模型：
 *  多 producer/consumer 共享同一映射，成员函数不同步，写入区间不重叠由上层保证。
 */
#include "core/ring/magic_ring.hpp"

#include <utility>

namespace salias::ring {
/// L1 ring 环形缓冲命名空间；magic ring 双映射访问的实现。

namespace {

/// 匿名命名空间：magic ring 实现内部辅助函数，仅本编译单元可见。

/**
 * @brief 判断 value 是否为非零 2 的幂。
 * @details 2 的幂是 magic ring 的硬性要求：容量为 2 的幂时，
 *  sequence 低位环绕可用位掩码 (pos & (cap-1)) 一步完成，比取模快且无分支。
 *  判据：value > 0 且 value 的二进制表示中只有一个 1 比特。
 * @param value 待测值。@retval true value 为非零 2 的幂。@retval false 否则。
 */
// 判断 value 是否为非零 2 的幂。
bool is_power_of_two(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

}  // namespace

/**
 * @brief 校验双映射并将其转换为持有映射的 ring。
 * @details 依次检查指针非空/长度非零（否则 ZeroLen）、容量为 2 的幂（否则
 *  NotPowerOfTwo）。校验通过后，记录起始指针与逻辑容量，并把 Mapping 所有权
 *  移交给构造出的 MagicRing。
 * @param mapping 已由 L0 成功创建的双映射。@return 校验通过的 ring 或错误码。
 */
// 校验双映射并将其转换为持有映射的 ring。
MagicRing::CreateResult MagicRing::create(platform::Mapping mapping) noexcept {
  if (mapping.as_ptr() == nullptr || mapping.len() == 0) {
    return std::unexpected(RingError::ZeroLen);
  }
  if (!is_power_of_two(mapping.len())) {
    return std::unexpected(RingError::NotPowerOfTwo);
  }

  auto* const base = mapping.as_ptr();
  const std::size_t cap = mapping.len();
  return MagicRing(std::move(mapping), base, cap);
}

/**
 * @brief 从双映射 ring 返回连续只读 span。
 * @details 通过 pos & mask 将逻辑位置环绕到环形索引；由于底层双映射，
 *  长度不超过 cap_ 时即使跨越逻辑边界也地址连续。空句柄或超长请求返回空 span。
 * @param pos 逻辑位置（通常为上层 sequence）。@param len 请求连续长度。
 * @return 指向有效映射的只读 span；非法时为空 span。
 */
// 从双映射 ring 返回连续只读 span。
std::span<const std::byte> MagicRing::slice(std::uint64_t pos, std::size_t len) const noexcept {
  if (base_ == nullptr || len > cap_) {
    return {};
  }
  // 安全性：create() 只接受 2 的幂容量，L0 保证 [base_, base_ + 2*cap_) 已映射。
  // 当 len <= cap_ 时，即使跨越逻辑容量边界，base_ + (pos & mask()) + len 也有效。
  return {base_ + (pos & mask()), len};
}

/**
 * @brief 从双映射 ring 返回连续可写 span。
 * @details 地址连续性约束与只读 slice() 完全相同；写入所有权由上层协议保证，
 *  L1 不建模。
 * @param pos 逻辑位置。@param len 请求连续长度。@return 可写 span；非法时为空。
 */
// 从双映射 ring 返回连续可写 span。
std::span<std::byte> MagicRing::slice_mut(std::uint64_t pos, std::size_t len) noexcept {
  if (base_ == nullptr || len > cap_) {
    return {};
  }
  // 安全性：地址连续性约束与 slice() 相同；写入所有权由上层协议保证，L1 不建模。
  return {base_ + (pos & mask()), len};
}

/**
 * @brief 保存已校验的映射、起始指针和逻辑容量。
 * @details 私有构造器，仅在 create() 校验通过后调用；接管 Mapping 所有权。
 * @param mapping 已校验的双映射。@param base 起始虚拟地址。@param cap 单段逻辑容量。
 */
// 保存已校验的映射、起始指针和逻辑容量。
MagicRing::MagicRing(platform::Mapping mapping, std::byte* base, std::size_t cap) noexcept
    : base_(base), cap_(cap), mapping_(std::move(mapping)) {}

}  // namespace salias::ring
