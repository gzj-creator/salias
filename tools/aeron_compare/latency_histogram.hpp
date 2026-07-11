/** @file tools/aeron_compare/latency_histogram.hpp
 * @brief 对比工具用的 latency（延迟）直方图，按对数分桶记录纳秒级延迟。
 * @details 本头文件位于 benchmark 工具层，本身不依赖 salias 核心分层（L0–L7），
 * 仅为 Aeron IPC 对照基准提供一套高分辨率、固定桶数的延迟采样与百分位查询工具。
 * 采用“前 64 桶精确到 1ns + 其后每 2 的幂次再细分为 64 个子桶”的混合分桶策略，
 * 兼顾低延迟区的精度与整体范围（覆盖到 uint64 最大值）。所有函数均无状态、
 * noexcept，桶数组由调用方持有，因此可安全地在多进程 mmap 共享内存中聚合。
 */
#pragma once

#include <time.h>

#include <cstddef>
#include <cstdint>
#include <limits>

namespace salias::tools::latency {
/// benchmark 工具内部命名空间，集中放置延迟直方图相关的纯函数与常量。

/// 前 64 桶为精确桶：下标 0..63 分别对应 0ns..63ns，每桶宽 1ns。
inline constexpr std::size_t kExactBucketCount = 64;
/// 每个二次幂区间（2^e 到 2^(e+1)）再细分的子桶数，决定对数区的分辨率。
inline constexpr std::size_t kSubBucketsPerPowerOfTwo = 64;
/// 总桶数 = 精确桶 + (63 - 6) 个二次幂区间 × 子桶数。
/// 从 2^6=64 起进入对数区，到 2^63 结束，故区间数为 (64 - 6)。
inline constexpr std::size_t kBucketCount = kExactBucketCount + (64 - 6) * kSubBucketsPerPowerOfTwo;

/**
 * @brief 将一个纳秒延迟值映射到对应的桶下标。
 * @param nanoseconds 待归类的延迟值，单位为纳秒。
 * @retval [0, kExactBucketCount) 精确区下标（延迟 < 64ns）。
 * @retval [kExactBucketCount, kBucketCount) 对数区下标。
 *
 * @details 小于 64ns 直接用原值作精确桶下标；否则用 __builtin_clzll 求出最高有效位
 * 得到二次幂指数 exponent，将 [2^exponent, 2^(exponent+1)) 区间均分为 64 个等宽子桶，
 * 落点即为其对数区下标。该映射保证单调且覆盖整个 uint64 值域。
 */
inline std::size_t bucket_index(std::uint64_t nanoseconds) noexcept {
  if (nanoseconds < kExactBucketCount) {
    return static_cast<std::size_t>(nanoseconds);
  }

  // __builtin_clzll 返回前导零个数，63 减之即得最高有效位的指数（二次幂的幂次）。
  const unsigned exponent = 63u - static_cast<unsigned>(__builtin_clzll(nanoseconds));
  const std::uint64_t base = std::uint64_t{1} << exponent;
  // 子桶宽度 = 该二次幂区间长度的一半再除以子桶数，保证对数区每幂次均匀细分。
  const std::uint64_t bucket_width = base / kSubBucketsPerPowerOfTwo;
  const std::uint64_t sub_bucket = (nanoseconds - base) / bucket_width;
  return kExactBucketCount + (exponent - 6u) * kSubBucketsPerPowerOfTwo +
         static_cast<std::size_t>(sub_bucket);
}

/**
 * @brief 反向查询：给定桶下标，返回其延迟上界（含）。
 * @param index 桶下标，取值范围为 [0, kBucketCount)。
 * @return 该桶对应的延迟上界（纳秒，闭区间）。
 * @retval uint64_max 当 index 为最后一个桶时，表示无上界（覆盖到最大延迟）。
 */
inline std::uint64_t bucket_upper_bound_ns(std::size_t index) noexcept {
  if (index < kExactBucketCount) {
    return index;
  }
  if (index >= kBucketCount - 1) {
    return std::numeric_limits<std::uint64_t>::max();
  }

  // 反推对数区：adjusted 去掉精确区偏移，再除/模子桶数得到幂次与子桶位置。
  const std::size_t adjusted = index - kExactBucketCount;
  const unsigned exponent = 6u + static_cast<unsigned>(adjusted / kSubBucketsPerPowerOfTwo);
  const std::uint64_t sub_bucket = adjusted % kSubBucketsPerPowerOfTwo;
  const std::uint64_t base = std::uint64_t{1} << exponent;
  const std::uint64_t bucket_width = base / kSubBucketsPerPowerOfTwo;
  return base + (sub_bucket + 1) * bucket_width - 1;
}

/**
 * @brief 向直方图记录一次延迟采样。
 * @param buckets 长度不小于 kBucketCount 的桶数组（由调用方持有）。
 * @param nanoseconds 本次延迟值，单位为纳秒。
 */
inline void record(std::uint64_t* buckets, std::uint64_t nanoseconds) noexcept {
  ++buckets[bucket_index(nanoseconds)];
}

/**
 * @brief 将 source 直方图逐桶累加合并到 destination。
 * @param destination 目标直方图（就地累加，需长度 >= kBucketCount）。
 * @param source 源直方图（只读，需长度 >= kBucketCount）。
 * @details 用于多消费者/多进程分别采样后在共享内存中聚合统计。
 */
inline void merge(std::uint64_t* destination, const std::uint64_t* source) noexcept {
  for (std::size_t index = 0; index < kBucketCount; ++index) {
    destination[index] += source[index];
  }
}

/**
 * @brief 在直方图上计算任意百分位对应的延迟上界。
 * @param buckets 已采样的桶数组（长度 >= kBucketCount）。
 * @param sample_count 样本总数，即所有桶计数之和。
 * @param numerator 分子，如求 p99 传 99。
 * @param denominator 分母，如求 p99 传 100（求 p99.9 则传 999/1000）。
 * @return 该百分位的延迟上界（纳秒）；无样本或参数非法时返回 0。
 *
 * @details 先用整数运算（向上取整）把 sample_count * numerator / denominator 换算成
 * 目标序号 target，再逐桶累加计数直至累计值达到 target，命中桶的上界即为该百分位。
 */
inline std::uint64_t percentile(const std::uint64_t* buckets, std::uint64_t sample_count,
                                std::uint32_t numerator, std::uint32_t denominator) noexcept {
  if (sample_count == 0 || denominator == 0 || numerator == 0 || numerator > denominator) {
    return 0;
  }

  // 整数除法会截断，这里用余数补一项并加 (denominator-1) 实现向上取整，避免百分位偏低。
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

/**
 * @brief 读取 CLOCK_MONOTONIC 时钟并换算为纳秒整数。
 * @return 当前单调时钟纳秒值；clock_gettime 失败时返回 0。
 * @note CLOCK_MONOTONIC 不受系统时间跳变影响，适合作为延迟测量的基准时间戳。
 */
inline std::uint64_t monotonic_now_ns() noexcept {
  timespec now{};
  if (::clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
    return 0;
  }
  return static_cast<std::uint64_t>(now.tv_sec) * 1'000'000'000u +
         static_cast<std::uint64_t>(now.tv_nsec);
}

}  // namespace salias::tools::latency
