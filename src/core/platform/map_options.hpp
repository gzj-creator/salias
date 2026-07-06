#pragma once

#include <cstddef>

namespace salias::platform {

enum class HugePage { None, Size2MB, Size1GB };

struct MapOptions {
  std::size_t size = 0;
  HugePage huge = HugePage::None;
  int numa_node = -1;
};

}  // namespace salias::platform
