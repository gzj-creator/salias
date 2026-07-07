#pragma once

#include <cstring>
#include <span>

#include "core/frame/header.hpp"

namespace salias::frame {

// 将字节数向上对齐到固定帧对齐粒度；对齐值为 2 的幂，热路径避免除法。
inline constexpr std::size_t align_up(std::size_t n) noexcept {
  return (n + (kFrameAlign - 1)) & ~(kFrameAlign - 1);
}

// 返回标准帧在环形区占用的总字节数：8 字节帧头加对齐后的 payload。
inline constexpr std::size_t frame_len(std::size_t payload_len) noexcept {
  return kHeaderSize + align_up(payload_len);
}

// 返回固定大小/无帧头模式的槽宽；是否允许该模式由上层通道决定。
inline constexpr std::size_t fixed_slot(std::size_t record_size) noexcept {
  return align_up(record_size);
}

// 将帧头编码到调用者提供的存储中；只处理字节布局，不负责同步发布。
inline void encode_header(std::span<std::byte, kHeaderSize> dst, std::uint32_t len,
                          std::uint32_t meta) noexcept {
  std::memcpy(dst.data(), &len, sizeof(len));
  std::memcpy(dst.data() + sizeof(len), &meta, sizeof(meta));
}

// 从调用者提供的存储中解码帧头；src 必须包含 kHeaderSize 个可读字节。
inline FrameHeader decode_header(std::span<const std::byte, kHeaderSize> src) noexcept {
  FrameHeader header{};
  std::memcpy(&header.len, src.data(), sizeof(header.len));
  std::memcpy(&header.meta, src.data() + sizeof(header.len), sizeof(header.meta));
  return header;
}

// 从 metadata 低字节提取帧标志。
inline std::uint32_t flags(const FrameHeader& header) noexcept {
  return header.meta & 0xFFu;
}

// 从 metadata 高位提取 generation/sequence 信息。
inline std::uint32_t seq(const FrameHeader& header) noexcept {
  return header.meta >> 8;
}

}  // namespace salias::frame
