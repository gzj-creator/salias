#pragma once

#include <cstddef>
#include <string>

namespace salias {

enum class Mode { Spsc, Mpsc, Broadcast, Bulk };
enum class WaitKind { SpinPause, Futex };

// Public configuration for Channel::create(). M0/M1 support only in-process SPSC with empty name;
// named cross-process driverless handshake is a later L7 milestone.
struct Config {
  std::string name;
  Mode mode = Mode::Spsc;
  std::size_t capacity = 1u << 20;
  bool fixed_size = false;
  std::size_t record_size = 0;
  WaitKind wait = WaitKind::SpinPause;
};

}  // namespace salias
