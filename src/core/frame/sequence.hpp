/**
 * @file src/core/frame/sequence.hpp
 * @brief sequence 序列号的拆分/重建：64 位全局序列号与帧头 metadata 的映射。
 * @details 本文件位于 L2 frame 层。salias 的 64 位全局 sequence 无法完整放入 8 字节
 *          帧头 metadata 的 24 位高位域（低 8 位为 flags），因此采取"低位入帧头、
 *          高位入旁路存储"的拆分策略：
 *          - 低 24 位随每帧写入 ring 的 metadata，供 consumer 快速定位与校验；
 *          - 高 40 位由上层 channel/ring 通过独立的原子位置变量维护，保证全局唯一。
 *          所有函数均为 inline constexpr/noexcept，热路径零开销，不涉及内存序
 *          （纯算术运算），跨进程安全。
 */
#pragma once

#include <cstdint>

namespace salias::frame {
/// sequence 序列号拆分/重建命名空间；纯算术、无原子、无副作用。

/// 存入帧头 metadata 高位的序列号位数：24 位。
inline constexpr std::uint32_t kSequenceLowBits = 24;
/// 低 24 位序列号掩码，用于截取/还原环绕(wraparound)后的低位。
inline constexpr std::uint32_t kSequenceLowMask = (1u << kSequenceLowBits) - 1u;

/**
 * @brief 取 64 位全局 sequence 的低 24 位。
 * @param sequence 全局序列号（单调递增，可能已远超 24 位范围）。
 * @return 低 24 位；超出部分自然被掩码截断，体现环绕(wraparound)语义。
 *
 * @note 仅做算术截断，不处理回绕判定的方向性（由上层 ring 比较位置时处理）。
 */
// 返回存入 8 字节帧头 metadata 高 24 位的序列号低位。
inline constexpr std::uint32_t sequence_low(std::uint64_t sequence) noexcept {
  return static_cast<std::uint32_t>(sequence) & kSequenceLowMask;
}

/**
 * @brief 从帧头 metadata 字中提取低 24 位序列号。
 * @param meta 32 位 metadata 字（低 8 位为 flags，高 24 位为 sequence 低位）。
 * @return 低 24 位序列号。
 */
// 从 metadata 右移 8 位去掉 flags 后，再掩码取低 24 位。
inline constexpr std::uint32_t sequence_low_from_meta(std::uint32_t meta) noexcept {
  return (meta >> 8) & kSequenceLowMask;
}

/**
 * @brief 将 64 位全局 sequence 拆分为帧头 metadata 低位与旁路高位。
 * @param sequence 输入的全局序列号。
 * @param[in,out] meta_inout 32 位 metadata 字；其低 8 位 flags 被保留，高 24 位
 *                           被覆写为 sequence 的低 24 位。
 * @param[out] sequence_high_out 输出 sequence 的高 40 位（用于旁路存储）。
 *
 * @note 用 `(meta_inout & 0xFFu)` 保护低 8 位 flags 不被破坏——flags 与 sequence
 *       共享同一个 metadata 字，编码时必须保留 flags。无内存序要求，纯算术。
 */
// 将 64 位序列号拆分：低 24 位写入 metadata 高位（保留低 8 位 flags），高 40 位旁路存储。
inline void encode_sequence(std::uint64_t sequence, std::uint32_t& meta_inout,
                            std::uint64_t& sequence_high_out) noexcept {
  meta_inout = (sequence_low(sequence) << 8) | (meta_inout & 0xFFu);
  sequence_high_out = sequence >> kSequenceLowBits;
}

/**
 * @brief 由帧头 metadata 与旁路高位重建 64 位全局 sequence。
 * @param meta 帧头 metadata 字（含低 24 位序列号）。
 * @param sequence_high 旁路存储的高 40 位。
 * @return 完整的 64 位全局序列号。
 *
 * @note 这是 encode_sequence 的逆运算。consumer 读取一帧后，结合本进程维护的
 *       高位即可恢复全局单调 sequence，用于位置校验与去重。
 */
// 由 metadata 低位与旁路高位拼接重建 64 位全局序列号。
inline constexpr std::uint64_t decode_sequence(std::uint32_t meta,
                                               std::uint64_t sequence_high) noexcept {
  return (sequence_high << kSequenceLowBits) | sequence_low_from_meta(meta);
}

}  // namespace salias::frame
