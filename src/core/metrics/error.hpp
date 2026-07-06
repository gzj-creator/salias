#pragma once

namespace salias::metrics {

enum class MetricsError {
  Ok = 0,
  RegionTooSmall,
  BadMagic,
  BadVersion,
  SlotOutOfRange,
  OpenFailed,
  MapFailed,
};

}  // namespace salias::metrics
