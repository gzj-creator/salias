#pragma once

namespace salias::platform {

enum class PlatformError {
  Ok = 0,
  MemfdCreateFailed,
  FtruncateFailed,
  ReserveFailed,
  MapFixedFailed,
  UnmapFailed,
  HugePageUnavailable,
  NumaUnavailable,
  InvalidSize,
};

}  // namespace salias::platform
