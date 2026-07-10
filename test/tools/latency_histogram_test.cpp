#include "tools/aeron_compare/latency_histogram.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>

namespace {

namespace latency = salias::tools::latency;

TEST(LatencyHistogramTest, MapsExactAndLogarithmicBucketBoundaries) {
  EXPECT_EQ(latency::bucket_index(0), 0u);
  EXPECT_EQ(latency::bucket_index(63), 63u);
  EXPECT_EQ(latency::bucket_index(64), 64u);
  EXPECT_EQ(latency::bucket_index(127), 127u);
  EXPECT_EQ(latency::bucket_index(128), 128u);
  EXPECT_EQ(latency::bucket_index(129), 128u);
  EXPECT_EQ(latency::bucket_upper_bound_ns(128), 129u);
}

TEST(LatencyHistogramTest, CalculatesNearestRankPercentiles) {
  std::array<std::uint64_t, latency::kBucketCount> buckets{};
  for (const std::uint64_t value : {10u, 20u, 30u, 40u, 50u}) {
    latency::record(buckets.data(), value);
  }

  EXPECT_EQ(latency::percentile(buckets.data(), 5, 50, 100), 30u);
  EXPECT_EQ(latency::percentile(buckets.data(), 5, 99, 100), 50u);
  EXPECT_EQ(latency::percentile(buckets.data(), 5, 999, 1000), 50u);
}

TEST(LatencyHistogramTest, MergesConsumerRowsBeforeCalculatingPercentiles) {
  std::array<std::uint64_t, latency::kBucketCount> first{};
  std::array<std::uint64_t, latency::kBucketCount> second{};
  std::array<std::uint64_t, latency::kBucketCount> merged{};
  latency::record(first.data(), 100);
  latency::record(first.data(), 200);
  latency::record(second.data(), 300);
  latency::record(second.data(), 400);

  latency::merge(merged.data(), first.data());
  latency::merge(merged.data(), second.data());

  EXPECT_EQ(latency::percentile(merged.data(), 4, 50, 100),
            latency::bucket_upper_bound_ns(latency::bucket_index(200)));
  EXPECT_EQ(latency::percentile(merged.data(), 4, 99, 100),
            latency::bucket_upper_bound_ns(latency::bucket_index(400)));
}

}  // namespace
