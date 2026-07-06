#pragma once

#include <cstddef>

namespace salias::ring {

inline constexpr std::size_t kCacheLine = 64;

template <class T>
struct alignas(kCacheLine) CacheAligned {
  T value;
};

}  // namespace salias::ring
