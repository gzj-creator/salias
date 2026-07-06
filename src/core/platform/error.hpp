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
  FutexFailed,
  InvalidSize,
};

}  // namespace salias::platform
