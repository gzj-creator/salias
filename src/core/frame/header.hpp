#pragma once

#include <cstddef>
#include <cstdint>

namespace salias::frame {

inline constexpr std::size_t kHeaderSize = 8;
inline constexpr std::size_t kFrameAlign = 8;

// 帧 metadata 标志；标准单帧消息使用 BEGIN | END | COMMITTED。
enum Flags : std::uint32_t {
  FLAG_BEGIN = 1u << 0,
  FLAG_END = 1u << 1,
  FLAG_PADDING = 1u << 2,
  FLAG_COMMITTED = 1u << 3,
};

// ring 内固定 8 字节帧头。
// salias 只面向同主机 Linux IPC，因此字段按主机小端布局写入；异构字节序传输不属于 L2 范围。
struct FrameHeader {
  std::uint32_t len;
  std::uint32_t meta;
};

static_assert(sizeof(FrameHeader) == kHeaderSize);

}  // namespace salias::frame
