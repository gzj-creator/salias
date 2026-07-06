#pragma once

#include <cstddef>

#include "core/platform/map_options.hpp"

namespace salias::channel {

struct ChannelConfig {
  std::size_t capacity = 1u << 20;
  platform::HugePage huge = platform::HugePage::None;
  int numa_node = -1;
  bool fixed_size = false;
  std::size_t record_size = 0;
};

}  // namespace salias::channel
