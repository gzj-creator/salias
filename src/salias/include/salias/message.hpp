#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace salias {

// 已接收消息的借用视图。
// payload 指向所属 Channel 的 ring，按通道协议在释放或覆盖前保持有效。
struct Message {
  std::span<const std::byte> payload;
  std::uint64_t position = 0;
  std::uint64_t next_position = 0;
  std::uint32_t meta = 0;
};

}  // namespace salias
