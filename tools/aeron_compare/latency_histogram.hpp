#pragma once

#include <time.h>

#include <cstddef>
#include <cstdint>
#include <limits>

namespace salias::tools::latency {

inline constexpr std::size_t kExactBucketCount = 64;
inline constexpr std::size_t kSubBucketsPerPowerOfTwo = 64;
inline constexpr std::size_t kBucketCount = kExactBucketCount + (64 - 6) * kSubBucketsPerPowerOfTwo;

inline std::size_t bucket_index(std::uint64_t nanoseconds) noexcept {
  if (nanoseconds < kExactBucketCount) {
    return static_cast<std::size_t>(nanoseconds);
  }

  const unsigned exponent = 63u - static_cast<unsigned>(__builtin_clzll(nanoseconds));
  const std::uint64_t base = std::uint64_t{1} << exponent;
  const std::uint64_t bucket_width = base / kSubBucketsPerPowerOfTwo;
  const std::uint64_t sub_bucket = (nanoseconds - base) / bucket_width;
  return kExactBucketCount + (exponent - 6u) * kSubBucketsPerPowerOfTwo +
         static_cast<std::size_t>(sub_bucket);
}

inline std::uint64_t bucket_upper_bound_ns(std::size_t index) noexcept {
  if (index < kExactBucketCount) {
    return index;
  }
  if (index >= kBucketCount - 1) {
    return std::numeric_limits<std::uint64_t>::max();
  }

  const std::size_t adjusted = index - kExactBucketCount;
  const unsigned exponent = 6u + static_cast<unsigned>(adjusted / kSubBucketsPerPowerOfTwo);
  const std::uint64_t sub_bucket = adjusted % kSubBucketsPerPowerOfTwo;
  const std::uint64_t base = std::uint64_t{1} << exponent;
  const std::uint64_t bucket_width = base / kSubBucketsPerPowerOfTwo;
  return base + (sub_bucket + 1) * bucket_width - 1;
}

inline void record(std::uint64_t* buckets, std::uint64_t nanoseconds) noexcept {
  ++buckets[bucket_index(nanoseconds)];
}

inline void merge(std::uint64_t* destination, const std::uint64_t* source) noexcept {
  for (std::size_t index = 0; index < kBucketCount; ++index) {
    destination[index] += source[index];
  }
}

inline std::uint64_t percentile(const std::uint64_t* buckets, std::uint64_t sample_count,
                                std::uint32_t numerator, std::uint32_t denominator) noexcept {
  if (sample_count == 0 || denominator == 0 || numerator == 0 || numerator > denominator) {
    return 0;
  }

  const std::uint64_t quotient = sample_count / denominator;
  const std::uint64_t remainder = sample_count % denominator;
  const std::uint64_t target =
      quotient * numerator + (remainder * numerator + denominator - 1) / denominator;
  std::uint64_t cumulative = 0;
  for (std::size_t index = 0; index < kBucketCount; ++index) {
    cumulative += buckets[index];
    if (cumulative >= target) {
      return bucket_upper_bound_ns(index);
    }
  }
  return bucket_upper_bound_ns(kBucketCount - 1);
}

inline std::uint64_t monotonic_now_ns() noexcept {
  timespec now{};
  if (::clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
    return 0;
  }
  return static_cast<std::uint64_t>(now.tv_sec) * 1'000'000'000u +
         static_cast<std::uint64_t>(now.tv_nsec);
}

}  // namespace salias::tools::latency
