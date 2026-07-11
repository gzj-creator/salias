/**
 * @file src/core/frame/codec.hpp
 * @brief frame 帧编解码：对齐计算、长度推导与帧头读写。
 * @details 本文件位于 L2 frame 层，提供 ring 中每帧的字节级布局运算与帧头
 *          (frame header)编解码。所有函数均为 inline constexpr/noexcept，热路径
 *          零开销、无异常、无同步原语——帧的可见性与发布顺序由上层 ring/channel
 *          的 atomic 内存序(release/acquire)保证，本层只负责字节搬运与位运算。
 *          关键不变式：帧头固定 8 字节(kHeaderSize)，payload 按 kFrameAlign(8B)
 *          向上对齐，consumer 因此可用对齐读并避免跨缓存行(cache line)拆分。
 *          线程/进程模型：编解码函数本身无状态、可并发调用；实际共享内存的读写
 *          由上层在已同步的内存区上进行。
 */
#pragma once

#include <cstring>
#include <span>

#include "core/frame/header.hpp"

namespace salias::frame {
/// frame 帧编解码命名空间；纯字节布局运算，无同步、无状态。

/**
 * @brief 将字节数 n 向上对齐到 kFrameAlign 粒度。
 * @param n 待对齐的字节数。
 * @return 不小于 n 且为 kFrameAlign 整数倍的最小值。
 *
 * @note 用位运算 `(n + (a-1)) & ~(a-1)` 替代除法，要求 a 为 2 的幂（编译期保证）。
 *       这是热路径上的高频运算，避免除法对延迟(latency)至关重要。
 */
// 将字节数向上对齐到固定帧对齐粒度；对齐值为 2 的幂，热路径避免除法。
inline constexpr std::size_t align_up(std::size_t n) noexcept {
  return (n + (kFrameAlign - 1)) & ~(kFrameAlign - 1);
}

/**
 * @brief 计算标准帧在 ring 中占用的总字节数。
 * @param payload_len 负载(payload)字节数（不含帧头）。
 * @return kHeaderSize + 对齐后的 payload_len，即该帧在 ring 中的槽宽。
 *
 * @note payload 经 align_up 对齐，保证下一帧帧头起始仍按 kFrameAlign 对齐。
 */
// 返回标准帧在环形区占用的总字节数：8 字节帧头加对齐后的 payload。
inline constexpr std::size_t frame_len(std::size_t payload_len) noexcept {
  return kHeaderSize + align_up(payload_len);
}

/**
 * @brief 返回固定大小/无帧头模式的槽宽。
 * @param record_size 单条定长记录的字节数。
 * @return 对齐到 kFrameAlign 后的记录宽度。
 *
 * @note 该模式用于所有记录等长的场景，可省去帧头；是否启用由上层 channel 决定。
 */
// 返回固定大小/无帧头模式的槽宽；是否允许该模式由上层通道决定。
inline constexpr std::size_t fixed_slot(std::size_t record_size) noexcept {
  return align_up(record_size);
}

/**
 * @brief 将帧头编码到调用者提供的 8 字节存储中。
 * @param dst 目标存储，span 静态保证恰好 kHeaderSize 字节。
 * @param len payload 长度。
 * @param meta metadata 字（低 8 位 flags + 高 24 位 sequence 低位）。
 *
 * @note 用 memcpy 而非直接指针解引用，避免违反严格别名(strict aliasing)与
 *       对齐要求；字节序为主机小端（同主机 IPC）。只处理字节布局，不负责
 *       同步发布——发布可见性由上层 ring 的 release 屏障保证。
 */
// 将帧头编码到调用者提供的存储中；只处理字节布局，不负责同步发布。
inline void encode_header(std::span<std::byte, kHeaderSize> dst, std::uint32_t len,
                          std::uint32_t meta) noexcept {
  std::memcpy(dst.data(), &len, sizeof(len));
  std::memcpy(dst.data() + sizeof(len), &meta, sizeof(meta));
}

/**
 * @brief 从调用者提供的 8 字节存储中解码帧头。
 * @param src 源存储，span 静态保证恰好 kHeaderSize 字节可读。
 * @return 填充好的 FrameHeader（len + meta）。
 *
 * @note memcpy 解码保证对任意对齐的 src 指针都安全；返回值拷贝为值语义，
 *       避免持有指向共享内存的指针。
 */
// 从调用者提供的存储中解码帧头；src 必须包含 kHeaderSize 个可读字节。
inline FrameHeader decode_header(std::span<const std::byte, kHeaderSize> src) noexcept {
  FrameHeader header{};
  std::memcpy(&header.len, src.data(), sizeof(header.len));
  std::memcpy(&header.meta, src.data() + sizeof(header.len), sizeof(header.meta));
  return header;
}

/**
 * @brief 从 metadata 低字节提取帧标志(flags)。
 * @param header 已解码的帧头。
 * @return flags 值（低 8 位），即 BEGIN/END/PADDING/COMMITTED 的组合。
 */
// 从 metadata 低字节提取帧标志。
inline std::uint32_t flags(const FrameHeader& header) noexcept {
  return header.meta & 0xFFu;
}

/**
 * @brief 从 metadata 高位提取 sequence 低位信息。
 * @param header 已解码的帧头。
 * @return 高 24 位的 sequence 低位（尚未与旁路高位拼成全局 sequence）。
 *
 * @note 完整 64 位 sequence 重建见 sequence.hpp 的 decode_sequence。
 */
// 从 metadata 高位提取 generation/sequence 信息。
inline std::uint32_t seq(const FrameHeader& header) noexcept {
  return header.meta >> 8;
}

}  // namespace salias::frame
