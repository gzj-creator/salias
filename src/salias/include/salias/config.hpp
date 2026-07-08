#pragma once

#include <cstddef>
#include <string>

namespace salias {

enum class Mode { Spsc, Mpsc, Broadcast, Bulk, Mpmc };
enum class WaitKind { SpinPause, Futex };

// Channel::create() 使用的公共配置。
// name 为空时创建进程内通道；当前公共 API 支持进程内 SPSC/MPSC/Broadcast/Bulk 和具名 SPSC/MPSC/MPMC。
struct Config {
  std::string name;
  Mode mode = Mode::Spsc;
  std::size_t capacity = 1u << 20;
  bool fixed_size = false;
  std::size_t record_size = 0;
  WaitKind wait = WaitKind::SpinPause;
};

}  // namespace salias
