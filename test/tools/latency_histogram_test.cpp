/**
 * @file test/tools/latency_histogram_test.cpp
 * @brief benchmark 工具层 latency（延迟）直方图的单元测试。
 * @details 本测试位于 benchmark 工具层之上，验证 latency_histogram.hpp 提供的
 *  混合分桶延迟直方图的核心函数。该直方图本身不依赖 salias 核心分层（L0–L7），
 *  仅为 Aeron IPC 对照基准提供延迟采样与百分位查询。测试覆盖三类语义：
 *  (1) bucket_index / bucket_upper_bound_ns 在精确区（<64ns）与对数区边界处的正确映射；
 *  (2) percentile 基于向上取整的 nearest-rank 百分位计算；
 *  (3) merge 逐桶累加合并后百分位结果的一致性，模拟多 consumer 在共享内存中聚合。
 *  所有被测函数均无状态、noexcept，桶数组由调用方持有。
 */
#include "tools/aeron_compare/latency_histogram.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>

namespace {
/// 匿名命名空间，隔离 latency_histogram 测试用例的链接符号到本编译单元。

/// benchmark 工具内部命名空间的别名，简化测试中 latency 域函数的引用。
namespace latency = salias::tools::latency;

/**
 * @brief 验证 bucket_index 在精确区与对数区边界处的映射，以及 bucket_upper_bound_ns
 *        的反向查询。
 * @details 0–63ns 落入精确区，下标即原值；64ns 起进入对数区。129ns 与 128ns
 *  映射到同一对数区桶（子桶宽度均分），且该桶上界为 129ns，验证分桶单调性与
 *  反查一致性。
 */
TEST(LatencyHistogramTest, MapsExactAndLogarithmicBucketBoundaries) {
  // 0ns 映射到精确桶下标 0。
  EXPECT_EQ(latency::bucket_index(0), 0u);
  // 63ns 是精确区最后一个桶。
  EXPECT_EQ(latency::bucket_index(63), 63u);
  // 64ns 起进入对数区第一个桶。
  EXPECT_EQ(latency::bucket_index(64), 64u);
  EXPECT_EQ(latency::bucket_index(127), 127u);
  EXPECT_EQ(latency::bucket_index(128), 128u);
  // 129ns 与 128ns 落在同一对数区子桶，故下标相同。
  EXPECT_EQ(latency::bucket_index(129), 128u);
  // 下标 128 对应的桶上界为 129ns（闭区间）。
  EXPECT_EQ(latency::bucket_upper_bound_ns(128), 129u);
}

/**
 * @brief 验证基于向上取整 nearest-rank 算法的百分位计算。
 * @details 向 5 个桶分别记录 10/20/30/40/50ns 各一次，样本总数 5。
 *  p50（5*50/100 向上取整 = 3）命中第 3 个样本 30ns；p99 命中最大值 50ns；
 *  p99.9（分母 1000）同样命中 50ns。验证整数向上取整与逐桶累计命中逻辑。
 */
TEST(LatencyHistogramTest, CalculatesNearestRankPercentiles) {
  std::array<std::uint64_t, latency::kBucketCount> buckets{};
  // 记录 5 个均匀分布的延迟样本，每个各一次。
  for (const std::uint64_t value : {10u, 20u, 30u, 40u, 50u}) {
    latency::record(buckets.data(), value);
  }

  // p50：目标序号 = ceil(5*50/100) = 3，命中第 3 个样本值 30ns。
  EXPECT_EQ(latency::percentile(buckets.data(), 5, 50, 100), 30u);
  // p99：命中最大样本值 50ns。
  EXPECT_EQ(latency::percentile(buckets.data(), 5, 99, 100), 50u);
  // p99.9（分母 1000）：同样命中最大样本值 50ns。
  EXPECT_EQ(latency::percentile(buckets.data(), 5, 999, 1000), 50u);
}

/**
 * @brief 验证多 consumer 采样经 merge 合并后再计算百分位的一致性。
 * @details 模拟两个 consumer 各自采样的直方图（first: 100/200ns，second: 300/400ns）
 *  经 merge 逐桶累加到 merged。合并后 4 个样本的 p50 应对应 200ns 所在桶上界，
 *  p99 应对应 400ns 所在桶上界。验证 merge + percentile 组合等价于在全量样本上
 *  直接计算百分位。
 */
TEST(LatencyHistogramTest, MergesConsumerRowsBeforeCalculatingPercentiles) {
  std::array<std::uint64_t, latency::kBucketCount> first{};
  std::array<std::uint64_t, latency::kBucketCount> second{};
  std::array<std::uint64_t, latency::kBucketCount> merged{};
  // 第一个 consumer 采样 100ns 与 200ns。
  latency::record(first.data(), 100);
  latency::record(first.data(), 200);
  // 第二个 consumer 采样 300ns 与 400ns。
  latency::record(second.data(), 300);
  latency::record(second.data(), 400);

  // 逐桶累加合并两个 consumer 的直方图到 merged。
  latency::merge(merged.data(), first.data());
  latency::merge(merged.data(), second.data());

  // 合并后 4 样本 p50 对应 200ns 所在桶的上界。
  EXPECT_EQ(latency::percentile(merged.data(), 4, 50, 100),
            latency::bucket_upper_bound_ns(latency::bucket_index(200)));
  // p99 对应 400ns 所在桶的上界（最大样本）。
  EXPECT_EQ(latency::percentile(merged.data(), 4, 99, 100),
            latency::bucket_upper_bound_ns(latency::bucket_index(400)));
}

}  // namespace
