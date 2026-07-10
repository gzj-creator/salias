#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace salias {

enum class Mode { FifoMpsc, FifoFanout, OrderedMpsc, OrderedFanout };
enum class HugePage { None, Size2MB, Size1GB };

// Channel<Mode>::create() 使用的公共 IPC 配置。
// name 必须非空；模板参数 Mode 决定协议，mode 字段仅保留给内部命名控制块兼容路径。
struct Config {
  std::string name;
  Mode mode = Mode::FifoMpsc;
  std::size_t capacity = 1u << 20;
  std::uint32_t num_producers = 1;
  std::uint32_t num_consumers = 1;
  HugePage huge = HugePage::None;
  bool fixed_size = false;
  std::size_t record_size = 0;
  // 发布流控窗口（字节）。0 表示不限制，生产者可领先消费者直至写满整个环。
  // 非 0 时把 “生产者领先消费者的在途字节数” 限制在 min(capacity, publication_window) 内，
  // 用于把稳态排队延迟从 “满环” 压到 “窗口大小”；不改变环的实际几何与回绕。
  std::size_t publication_window = 0;
};

}  // namespace salias
