#pragma once

namespace salias::metrics {

enum class MetricsError {
  Ok = 0,
  RegionTooSmall,
  BadMagic,
  BadVersion,
  SlotOutOfRange,
};

}  // namespace salias::metrics
