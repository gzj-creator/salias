/**
 * @file src/core/platform/monotonic_clock.hpp
 * @brief Host-wide monotonic clock access for same-machine IPC metadata.
 */
#pragma once

#include <cstdint>
#include <ctime>
#include <limits>
#include <optional>

namespace salias::platform {

inline std::optional<std::uint64_t> monotonic_now_ns() noexcept {
  timespec value{};
  if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 || value.tv_sec < 0 || value.tv_nsec < 0 ||
      value.tv_nsec >= 1'000'000'000) {
    return std::nullopt;
  }
  const auto seconds = static_cast<std::uint64_t>(value.tv_sec);
  constexpr std::uint64_t kNanosPerSecond = 1'000'000'000;
  if (seconds >
      (std::numeric_limits<std::uint64_t>::max() - static_cast<std::uint64_t>(value.tv_nsec)) /
          kNanosPerSecond) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return seconds * kNanosPerSecond + static_cast<std::uint64_t>(value.tv_nsec);
}

}  // namespace salias::platform
