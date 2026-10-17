/**
 * @file bench/channel_stress.cpp
 * @brief salias 通道(channel)层的压力与吞吐 stress benchmark。
 * @details 位于 benchmark(bench)工具层，调用 L7 公共 API(salias::Channel / Publisher /
 *          Subscriber)对 L5 channel 通道进行端到端压力测量。本文件覆盖以下场景：
 *          (1) 同线程内单生产者单消费者(SPSC)发布/接收往返延迟(BM_fifo_roundtrip_64b)，
 *              度量单条 64 字节 payload 的发布-提交-消费往返耗时；
 *          (2) 四生产者单消费者(MPSC)并发吞吐(BM_four_producers_64b)，在 FIFO 与
 *              Ordered 两种协议模式下度量多线程争用下的稳定吞吐与校验和正确性。
 *          (3) LatestFanout 50%/90% 水位下的即时消费、停滞消费者持续丢旧与并发扇出。
 *          所有 benchmark 使用 Google Benchmark 框架，通过 BENCHMARK_MAIN() 注册并运行。
 *          关键不变式：MPSC 场景消费完成后以 producer_sum / seq_sum 校验和断言消息无丢失、
 *          无重复、各生产者序列号连续。线程模型：生产者以独立 std::thread 运行，
 *          消费者为主 benchmark 线程；通过 std::atomic<bool> start 屏障同步起跑。
 */
#include <benchmark/benchmark.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include "salias/salias.hpp"

/// 匿名命名空间，仅本编译单元可见的 stress benchmark 辅助类型与函数。
namespace {

/**
 * @brief stress benchmark 使用的固定 64 字节负载结构。
 * @details 内存布局：producer(4B) + seq(4B) + padding(56B)，合计 64 字节，
 *          恰好占用一个典型缓存行(cache line)的一部分，便于在多生产者场景下
 *          通过 producer/seq 字段验证投递正确性。padding 使结构体对齐到 64 字节，
 *          模拟真实场景中常见的定长小消息。所有权：值语义，可平凡拷贝(memcpy)。
 */
struct SmallPayload {
  std::uint32_t producer = 0;  ///< 生产者编号，用于校验和核对各生产者投递完整性。
  std::uint32_t seq = 0;       ///< 该生产者发出的消息序列号，从 0 递增。
  std::array<std::byte, 56> padding{};  ///< 填充(padding)至 64 字节，模拟定长负载。
};

struct KeyedPayload {
  std::uint64_t sequence = 0;
  std::uint64_t inverse = 0;
  std::array<std::byte, 48> padding{};
};

/**
 * @brief 按 benchmark 模式和容量构造公共通道配置。
 * @param mode 通道协议模式(FIFO/Ordered、MPSC/Fanout)。
 * @param capacity 环形缓冲(ring)容量(字节)，默认 1MiB，需为 2 的幂。
 * @param producers 预留的生产者(producer)数量，用于预分配 per-producer 提交槽位。
 * @return 构造好的 salias::Config，name 含 PID 与模式编号以保证跨进程唯一且可复用。
 * @note name 中嵌入 ::getpid() 是为了在共享内存(shared memory)命名空间中避免与
 *       其它并发运行的 benchmark 进程冲突。
 */
// 按 benchmark 模式和容量构造公共通道配置。
salias::Config config_for(salias::Mode mode, std::size_t capacity = 1u << 20,
                          std::uint32_t producers = 1) {
  salias::Config config;
  config.name = "salias-bench-stress-" + std::to_string(::getpid()) + "-" +
                std::to_string(static_cast<int>(mode));
  config.mode = mode;
  config.capacity = capacity;
  config.num_producers = producers;
  return config;
}

/**
 * @brief 持续重试发布，直到成功或遇到非背压(backpressure)错误。
 * @tparam M 通道协议模式。
 * @param publisher 已绑定到某通道的发布端。
 * @param payload 待发布的负载(payload)视图。
 * @retval true 发布成功。
 * @retval false 遭遇非背压错误(如 MessageTooLarge)，不可重试。
 * @note 遇 BackPressured 时调用 std::this_thread::yield() 退避(backoff)让出 CPU，
 *       等待消费者释放环空间后再重试；这模拟真实应用中"发不出去就等"的语义。
 *       offer 内部已做拷贝并 commit，故成功即对消费者立即可见(acquire/release 语义)。
 */
// 持续重试发布，直到成功或遇到非背压错误。
template <salias::Mode M>
bool offer_until_accepted(salias::Publisher<M>& publisher, std::span<const std::byte> payload) {
  for (;;) {
    auto offered = publisher.offer(payload);
    if (offered && offered.value()) {
      return true;
    }
    if (!offered && offered.error() != salias::Error::BackPressured) {
      return false;
    }
    // 背压：环形缓冲已满或流控窗口(flow window)在途字节数达上限，让出 CPU 等消费者回收。
    std::this_thread::yield();
  }
}

/**
 * @brief 解码 stress benchmark 使用的固定小 payload。
 * @param payload 已接收消息的负载视图(指向通道 ring 内存)。
 * @return 拷贝出的 SmallPayload 值。
 * @note 用 std::memcpy 按字节拷贝以避免严格别名(strict aliasing)问题；
 *       拷贝后即与共享内存解耦，后续校验和计算不受生产者覆盖影响。
 */
// 解码 stress benchmark 使用的固定小 payload。
SmallPayload decode_small(std::span<const std::byte> payload) {
  SmallPayload value{};
  std::memcpy(&value, payload.data(), sizeof(value));
  return value;
}

/**
 * @brief 测量同线程内 64 字节 payload 的 SPSC 发布/接收往返延迟。
 * @param state Google Benchmark 状态，每次迭代对应一次"发布-消费"往返。
 * @details 同一线程既作生产者(producer)又作消费者(consumer)，逐条发布后立即接收，
 *          度量单条消息的端到端延迟(offer 提交 -> try_recv 可见 -> release 回收)。
 *          覆盖 L3 flow 层发布与消费的热路径，验证最简场景下 ring 的正确性：
 *          每条已提交消息必须立即可被同一消费者读到，否则记为错误。
 */
// 测量同线程内 64 字节 payload 的 SPSC 发布/接收往返。
void BM_fifo_roundtrip_64b(benchmark::State& state) {
  auto channel_result = salias::FifoMpscChannel::create(config_for(salias::Mode::FifoMpsc));
  if (!channel_result) {
    state.SkipWithError("failed to create SPSC channel");
    return;
  }

  auto channel = std::move(channel_result).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  SmallPayload payload{};

  for (auto _ : state) {
    if (!offer_until_accepted(publisher, std::as_bytes(std::span{&payload, 1}))) {
      state.SkipWithError("SPSC offer failed");
      break;
    }
    auto message = subscriber.try_recv();
    if (!message) {
      // 提交成功的消息必须立即可见；读不到说明发布/消费进度推进有误。
      state.SkipWithError("SPSC receive missed committed message");
      break;
    }
    // 阻止编译器优化掉对 payload 的读取，确保真实发生内存访问。
    benchmark::DoNotOptimize(message->payload.data());
    // 释放消息占用的环空间，使生产者可回收复用该 ring 槽位。
    subscriber.release(*message);
  }

  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(sizeof(payload)));
}

void BM_ttl_expiration_64b(benchmark::State& state) {
  auto channel_result = salias::FifoMpscChannel::create(config_for(salias::Mode::FifoMpsc));
  if (!channel_result) {
    state.SkipWithError("failed to create TTL channel");
    return;
  }
  auto channel = std::move(*channel_result);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  SmallPayload payload{};

  for (auto _ : state) {
    const auto bytes = std::as_bytes(std::span{&payload, 1});
    auto expired_offer = publisher.offer_for(bytes, std::chrono::nanoseconds{0});
    if (!expired_offer || !*expired_offer || subscriber.try_recv()) {
      state.SkipWithError("TTL expiration path failed to skip an expired frame");
      return;
    }
    auto live_offer = publisher.offer(bytes);
    if (!live_offer || !*live_offer) {
      state.SkipWithError("TTL benchmark live offer failed");
      return;
    }
    auto message = subscriber.try_recv();
    if (!message || decode_small(message->payload).seq != payload.seq) {
      state.SkipWithError("TTL benchmark live frame mismatch");
      return;
    }
    benchmark::DoNotOptimize(message->payload.data());
    subscriber.release(*message);
    ++payload.seq;
  }

  if (subscriber.expired_messages() != static_cast<std::uint64_t>(state.iterations())) {
    state.SkipWithError("TTL expiration counter mismatch");
    return;
  }
  state.SetItemsProcessed(state.iterations() * 2);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(sizeof(payload) * 2));
}

/**
 * @brief 测量四个生产者(producer)线程和一个消费者(consumer)下的 MPSC 吞吐。
 * @tparam M 通道协议模式(由 BENCHMARK_TEMPLATE 实例化为 FifoMpsc / OrderedMpsc)。
 * @param state Google Benchmark 状态；state.range(0) 为每个生产者应发送的消息数。
 * @details 每次 benchmark 迭代重新创建通道，确保各次测量互不干扰。
 *          线程模型：4 个生产者线程并发发布，主线程作为单消费者收集全部消息。
 *          起跑同步：所有生产者自旋(spin)等待 std::atomic<bool> start 置位，
 *          以 acquire/release 语义保证"看到 start=true 即可安全开始发布"，
 *          避免线程启动抖动污染吞吐测量。
 *          正确性校验：消费完成后用 producer_sum / seq_sum 与闭式公式算出的期望值比对，
 *          断言消息无丢失、无重复、各生产者序列号(sequence)连续——这是无锁 MPSC
 *          正确性的强校验。失败标记通过 std::atomic<bool> failed 跨线程传递。
 */
// 测量四个生产者线程和一个消费者下的 MPSC 吞吐。
template <salias::Mode M>
void BM_four_producers_64b(benchmark::State& state) {
  constexpr std::uint32_t kProducerCount = 4;
  const auto messages_per_producer = static_cast<std::uint32_t>(state.range(0));
  const std::uint64_t total_messages =
      static_cast<std::uint64_t>(kProducerCount) * messages_per_producer;

  for (auto _ : state) {
    state.PauseTiming();
    // 每次迭代新建通道，容量 1MiB，预分配 4 个 per-producer 提交槽位。
    auto channel_result = salias::Channel<M>::create(config_for(M, 1u << 20, kProducerCount));
    if (!channel_result) {
      state.SkipWithError("failed to create MPSC channel");
      return;
    }
    auto channel = std::move(channel_result).value();
    auto subscriber = channel.subscriber();
    std::atomic<bool> start{false};  ///< 起跑屏障：所有生产者等待此标志置位后才开始发布。
    std::atomic<bool> failed{false};  ///< 失败标志：任一生产者发布不可重试错误时置位。
    std::vector<std::thread> producers;
    producers.reserve(kProducerCount);

    // 为每个生产者编号启动一个线程；线程内创建独立 publisher 后自旋等待起跑。
    for (std::uint32_t producer_id = 0; producer_id < kProducerCount; ++producer_id) {
      producers.emplace_back([producer_id, messages_per_producer, &channel, &start, &failed] {
        auto publisher = channel.publisher();
        // 自旋(spin)等待起跑信号；acquire 语义与主线程的 release 配对，保证可见性。
        while (!start.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        for (std::uint32_t seq = 0; seq < messages_per_producer; ++seq) {
          SmallPayload payload{.producer = producer_id, .seq = seq};
          if (!offer_until_accepted(publisher, std::as_bytes(std::span{&payload, 1}))) {
            failed.store(true, std::memory_order_release);
            return;
          }
        }
      });
    }

    std::uint64_t producer_sum = 0;  ///< 累加所有收到消息的 producer 字段，用于校验和。
    std::uint64_t seq_sum = 0;  ///< 累加所有收到消息的 seq 字段，用于校验和。
    state.ResumeTiming();
    // release 语义发布起跑信号，与生产者的 acquire 配对，确保线程同步无数据竞争。
    start.store(true, std::memory_order_release);

    // 单消费者循环：逐条 try_recv，无消息时 yield 退避，直至收满 total_messages。
    for (std::uint64_t received = 0; received < total_messages;) {
      auto message = subscriber.try_recv();
      if (!message) {
        std::this_thread::yield();
        continue;
      }
      const SmallPayload payload = decode_small(message->payload);
      producer_sum += payload.producer;
      seq_sum += payload.seq;
      ++received;
      // 释放环空间，使生产者得以回收该 ring 槽位继续发布(背压解除)。
      subscriber.release(*message);
    }

    for (auto& producer : producers) {
      producer.join();
    }
    state.PauseTiming();

    // 期望校验和用等差数列闭式公式计算，避免 O(N) 累加误差与开销。
    // producer_sum: 每个生产者发送 0..(kProducerCount-1) 各 messages_per_producer 次，
    //   即 messages_per_producer * sum(0..kProducerCount-1)。
    const std::uint64_t expected_producer_sum = static_cast<std::uint64_t>(messages_per_producer) *
                                                (kProducerCount - 1) * kProducerCount / 2;
    // seq_sum: 每个生产者发送 0..(messages_per_producer-1) 各 kProducerCount 次，
    //   即 kProducerCount * sum(0..messages_per_producer-1)。
    const std::uint64_t expected_seq_sum = static_cast<std::uint64_t>(kProducerCount) *
                                           (messages_per_producer - 1) * messages_per_producer / 2;
    if (failed.load(std::memory_order_acquire) || producer_sum != expected_producer_sum ||
        seq_sum != expected_seq_sum) {
      // 校验和不匹配意味着丢消息、重复或乱序——对无锁 MPSC 是致命错误。
      state.SkipWithError("MPSC checksum mismatch");
      return;
    }
    state.ResumeTiming();
  }

  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(total_messages));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(total_messages) *
                          static_cast<std::int64_t>(sizeof(SmallPayload)));
}

template <std::uint32_t Watermark>
void BM_latest_fast_subscriber_64b(benchmark::State& state) {
  using LatestChannel = salias::LatestFanoutChannel<1u << 16, Watermark>;
  auto created = LatestChannel::create(config_for(salias::Mode::LatestFanout));
  if (!created) {
    state.SkipWithError("failed to create latest fanout channel");
    return;
  }
  auto publisher = created->publisher();
  auto subscriber = created->subscriber();
  SmallPayload payload{};
  for (auto _ : state) {
    auto offered = publisher.offer(std::as_bytes(std::span{&payload, 1}));
    if (!offered) {
      state.SkipWithError("latest fast-path offer failed");
      return;
    }
    auto message = subscriber.try_recv();
    if (!message || decode_small(message->payload).seq != payload.seq) {
      state.SkipWithError("latest fast-path payload mismatch");
      return;
    }
    benchmark::DoNotOptimize(message->payload.data());
    subscriber.release(*message);
    ++payload.seq;
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(sizeof(payload)));
}

template <bool ChurnSubscriber>
void BM_fanout_registration_roundtrip_64b(benchmark::State& state) {
  auto config = config_for(salias::Mode::FifoFanout, 4096);
  auto created = salias::FifoFanoutChannel::create(config);
  if (!created) {
    state.SkipWithError("failed to create fifo fanout channel");
    return;
  }
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  SmallPayload payload{};

  auto exchange = [&](salias::FifoFanoutSubscriber& subscriber) {
    auto offered = publisher.offer(std::as_bytes(std::span{&payload, 1}));
    if (!offered) {
      state.SkipWithError("fifo fanout offer failed");
      return false;
    }
    auto message = subscriber.try_recv();
    if (!message || decode_small(message->payload).seq != payload.seq) {
      state.SkipWithError("fifo fanout roundtrip payload mismatch");
      return false;
    }
    benchmark::DoNotOptimize(message->payload.data());
    subscriber.release(*message);
    ++payload.seq;
    return true;
  };

  if constexpr (ChurnSubscriber) {
    for (auto _ : state) {
      auto subscriber = channel.try_subscriber();
      if (!subscriber) {
        state.SkipWithError("subscriber registration failed during churn");
        return;
      }
      if (!exchange(*subscriber)) {
        return;
      }
    }
  } else {
    auto subscriber = channel.subscriber();
    for (auto _ : state) {
      if (!exchange(subscriber)) {
        return;
      }
    }
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(sizeof(payload)));
}

template <salias::SubscriptionPolicy Policy>
void BM_latest_policy_roundtrip_64b(benchmark::State& state) {
  using LatestChannel = salias::LatestFanoutChannel<4096, 90>;
  auto created = LatestChannel::create(config_for(salias::Mode::LatestFanout, 4096));
  if (!created) {
    state.SkipWithError("failed to create latest fanout channel");
    return;
  }
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.try_subscriber(Policy);
  if (!subscriber) {
    state.SkipWithError("latest subscriber registration failed");
    return;
  }
  SmallPayload payload{};
  for (auto _ : state) {
    auto offered = publisher.offer(std::as_bytes(std::span{&payload, 1}));
    if (!offered) {
      state.SkipWithError("latest policy roundtrip offer failed");
      return;
    }
    auto message = subscriber->try_recv();
    if (!message || decode_small(message->payload).seq != payload.seq) {
      state.SkipWithError("latest policy roundtrip payload mismatch");
      return;
    }
    benchmark::DoNotOptimize(message->payload.data());
    subscriber->release(*message);
    ++payload.seq;
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(sizeof(payload)));
}

// Keep the subscriber idle through many wraps, then verify the entire retained suffix.
template <std::uint32_t Watermark>
void BM_latest_slow_subscriber_64b(benchmark::State& state) {
  constexpr std::size_t kCapacity = 1u << 16;
  constexpr std::size_t kRetention = (kCapacity * Watermark) / 100;
  constexpr std::size_t kFrameSize = sizeof(SmallPayload) + 8;
  using LatestChannel = salias::LatestFanoutChannel<kCapacity, Watermark>;
  auto created = LatestChannel::create(config_for(salias::Mode::LatestFanout));
  if (!created) {
    state.SkipWithError("failed to create latest fanout channel");
    return;
  }
  auto publisher = created->publisher();
  auto subscriber = created->subscriber();
  const auto messages_per_iteration = static_cast<std::uint32_t>(state.range(0));
  std::uint64_t published = 0;
  for (auto _ : state) {
    for (std::uint32_t index = 0; index < messages_per_iteration; ++index) {
      const SmallPayload payload{.seq = static_cast<std::uint32_t>(published)};
      if (!publisher.offer(std::as_bytes(std::span{&payload, 1}))) {
        state.SkipWithError("idle latest subscriber unexpectedly backpressured the writer");
        return;
      }
      ++published;
    }
  }
  const std::uint64_t retained = std::min<std::uint64_t>(published, kRetention / kFrameSize);
  std::uint64_t expected = published - retained;
  while (auto message = subscriber.try_recv()) {
    if (message->sequence != expected ||
        decode_small(message->payload).seq != static_cast<std::uint32_t>(expected)) {
      state.SkipWithError("latest retained suffix mismatch");
      return;
    }
    ++expected;
    subscriber.release(*message);
  }
  if (expected != published) {
    state.SkipWithError("latest retained suffix is incomplete");
    return;
  }
  state.counters["retained"] = static_cast<double>(retained);
  state.counters["dropped"] = static_cast<double>(published - retained);
  state.SetItemsProcessed(static_cast<std::int64_t>(published));
  state.SetBytesProcessed(static_cast<std::int64_t>(published * sizeof(SmallPayload)));
}

void BM_latest_message_window_64b(benchmark::State& state) {
  constexpr std::uint64_t kMessageWindow = 64;
  using LatestChannel = salias::LatestFanoutChannel<1u << 16, 90>;
  auto config = config_for(salias::Mode::LatestFanout);
  config.publication_window_messages = kMessageWindow;
  auto created = LatestChannel::create(config);
  if (!created) {
    state.SkipWithError("failed to create message-window latest channel");
    return;
  }
  auto publisher = created->publisher();
  auto subscriber = created->subscriber();
  const auto messages_per_iteration = static_cast<std::uint32_t>(state.range(0));
  std::uint64_t published = 0;
  for (auto _ : state) {
    for (std::uint32_t index = 0; index < messages_per_iteration; ++index) {
      const SmallPayload payload{.seq = static_cast<std::uint32_t>(published)};
      if (!publisher.offer(std::as_bytes(std::span{&payload, 1}))) {
        state.SkipWithError("message-window latest offer failed");
        return;
      }
      ++published;
    }
  }

  const auto retained = std::min(published, kMessageWindow);
  const auto lost = published - retained;
  auto lag = subscriber.try_recv_checked();
  if ((lost != 0 && (lag || lag.error() != salias::Error::Lagged)) ||
      subscriber.lagged_messages() != lost) {
    state.SkipWithError("message-window latest lag accounting mismatch");
    return;
  }
  std::uint64_t expected = lost;
  while (auto message = subscriber.try_recv()) {
    if (message->sequence != expected ||
        decode_small(message->payload).seq != static_cast<std::uint32_t>(expected)) {
      state.SkipWithError("message-window latest retained suffix mismatch");
      return;
    }
    ++expected;
    subscriber.release(*message);
  }
  if (expected != published) {
    state.SkipWithError("message-window latest retained suffix is incomplete");
    return;
  }
  state.counters["retained"] = static_cast<double>(retained);
  state.counters["dropped"] = static_cast<double>(lost);
  state.SetItemsProcessed(static_cast<std::int64_t>(published));
  state.SetBytesProcessed(static_cast<std::int64_t>(published * sizeof(SmallPayload)));
}

// One active subscriber races the writer; the second remains idle and must not block either.
template <std::uint32_t Watermark>
void BM_latest_concurrent_fanout_64b(benchmark::State& state) {
  using LatestChannel = salias::LatestFanoutChannel<1u << 16, Watermark>;
  const auto messages = static_cast<std::uint32_t>(state.range(0));
  std::uint64_t received_total = 0;
  std::uint64_t retries_total = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto config = config_for(salias::Mode::LatestFanout);
    config.num_consumers = 2;
    auto created = LatestChannel::create(config);
    if (!created) {
      state.SkipWithError("failed to create concurrent latest fanout channel");
      return;
    }
    auto publisher = created->publisher();
    auto subscriber = created->subscriber();
    auto idle_subscriber = created->subscriber();
    benchmark::DoNotOptimize(idle_subscriber);
    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::atomic<bool> failed{false};
    std::uint64_t retries = 0;
    std::thread producer([&] {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (std::uint32_t sequence = 0;
           sequence < messages && !stop.load(std::memory_order_relaxed);) {
        const SmallPayload payload{.seq = sequence};
        auto offered = publisher.offer(std::as_bytes(std::span{&payload, 1}));
        if (offered) {
          ++sequence;
        } else if (offered.error() == salias::Error::BackPressured) {
          ++retries;
          std::this_thread::yield();
        } else {
          failed.store(true, std::memory_order_relaxed);
          break;
        }
      }
    });
    std::uint64_t next_sequence = 0;
    std::uint64_t received = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    state.ResumeTiming();
    start.store(true, std::memory_order_release);
    while (next_sequence < messages && !failed.load(std::memory_order_relaxed)) {
      auto message = subscriber.try_recv();
      if (!message) {
        if (std::chrono::steady_clock::now() >= deadline) {
          failed.store(true, std::memory_order_relaxed);
          break;
        }
        std::this_thread::yield();
        continue;
      }
      const auto payload = decode_small(message->payload);
      if (payload.seq < next_sequence || payload.seq >= messages ||
          message->sequence != payload.seq) {
        failed.store(true, std::memory_order_relaxed);
      }
      next_sequence = std::uint64_t{payload.seq} + 1;
      ++received;
      subscriber.release(*message);
    }
    stop.store(true, std::memory_order_relaxed);
    producer.join();
    state.PauseTiming();
    if (failed.load(std::memory_order_relaxed) || next_sequence != messages) {
      state.SkipWithError("concurrent latest fanout delivery failed or timed out");
      return;
    }
    received_total += received;
    retries_total += retries;
    state.ResumeTiming();
  }
  const auto published = state.iterations() * messages;
  state.counters["received"] = static_cast<double>(received_total);
  state.counters["dropped"] = static_cast<double>(published - received_total);
  state.counters["retries"] = static_cast<double>(retries_total);
  state.SetItemsProcessed(published);
  state.SetBytesProcessed(published * static_cast<std::int64_t>(sizeof(SmallPayload)));
}

void BM_latest_checked_roundtrip_64b(benchmark::State& state) {
  auto created =
      salias::LatestFanoutChannel<1u << 16>::create(config_for(salias::Mode::LatestFanout));
  if (!created) {
    state.SkipWithError("failed to create checked latest channel");
    return;
  }
  auto publisher = created->publisher();
  auto subscriber = created->subscriber();
  SmallPayload payload{};
  for (auto _ : state) {
    if (!publisher.offer(std::as_bytes(std::span{&payload, 1}))) {
      state.SkipWithError("checked roundtrip offer failed");
      return;
    }
    auto message = subscriber.try_recv_checked();
    if (!message || !*message || decode_small((**message).payload).seq != payload.seq) {
      state.SkipWithError("checked roundtrip receive failed");
      return;
    }
    benchmark::DoNotOptimize((**message).payload.data());
    subscriber.release(**message);
    ++payload.seq;
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(sizeof(payload)));
}

// Seek after each burst, checking that loss + skips + delivery account for every frame.
void BM_latest_seek_64b(benchmark::State& state) {
  auto created =
      salias::LatestFanoutChannel<1u << 16>::create(config_for(salias::Mode::LatestFanout));
  if (!created) {
    state.SkipWithError("failed to create latest seek channel");
    return;
  }
  auto publisher = created->publisher();
  auto subscriber = created->subscriber();
  const auto burst = static_cast<std::uint32_t>(state.range(0));
  std::uint64_t published = 0;
  for (auto _ : state) {
    for (std::uint32_t index = 0; index < burst; ++index) {
      const SmallPayload payload{.seq = static_cast<std::uint32_t>(published)};
      if (!publisher.offer(std::as_bytes(std::span{&payload, 1}))) {
        state.SkipWithError("latest seek offer failed");
        return;
      }
      ++published;
    }
    auto skipped = subscriber.skip_to_latest();
    auto message = subscriber.try_recv_checked();
    if (!skipped || *skipped != burst - 1 || !message || !*message ||
        (**message).sequence != published - 1 ||
        decode_small((**message).payload).seq != static_cast<std::uint32_t>(published - 1)) {
      state.SkipWithError("latest seek suffix mismatch");
      return;
    }
    subscriber.release(**message);
  }
  if (subscriber.lagged_messages() + subscriber.skipped_messages() + state.iterations() !=
      published) {
    state.SkipWithError("latest seek loss accounting mismatch");
    return;
  }
  state.counters["lagged"] = static_cast<double>(subscriber.lagged_messages());
  state.counters["skipped"] = static_cast<double>(subscriber.skipped_messages());
  state.SetItemsProcessed(static_cast<std::int64_t>(published));
  state.SetBytesProcessed(static_cast<std::int64_t>(published * sizeof(SmallPayload)));
}

void BM_keyed_latest_roundtrip_64b(benchmark::State& state) {
  using Keyed = salias::KeyedLatestChannel<std::uint64_t, KeyedPayload, 1024>;
  const auto name = "salias-bench-keyed-" + std::to_string(::getpid()) + "-" +
                    std::to_string(state.thread_index());
  auto created = Keyed::create(name);
  if (!created) {
    state.SkipWithError("failed to create keyed latest channel");
    return;
  }
  auto publisher = created->publisher();
  auto subscriber = created->subscriber();
  KeyedPayload payload{};
  constexpr std::uint64_t kKey = 17;

  for (auto _ : state) {
    payload.inverse = ~payload.sequence;
    auto offered = publisher.offer(kKey, payload);
    if (!offered) {
      state.SkipWithError("keyed latest offer failed");
      return;
    }
    auto received = subscriber.try_recv_checked();
    if (!received || !*received || (**received).key != kKey ||
        (**received).value.sequence != payload.sequence ||
        (**received).value.inverse != ~payload.sequence) {
      state.SkipWithError("keyed latest snapshot mismatch");
      return;
    }
    benchmark::DoNotOptimize((**received).value.sequence);
    ++payload.sequence;
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(sizeof(payload)));
}

void BM_priority_weighted_roundtrip_64b(benchmark::State& state) {
  using PriorityPublisher =
      salias::PriorityPublisher<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc>;
  using PrioritySubscriber =
      salias::PrioritySubscriber<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc>;
  auto high_config = config_for(salias::Mode::FifoMpsc, 1u << 16);
  high_config.name += "-priority-high";
  auto low_config = config_for(salias::Mode::FifoMpsc, 1u << 16);
  low_config.name += "-priority-low";
  auto high = salias::FifoMpscChannel::create(high_config);
  auto low = salias::FifoMpscChannel::create(low_config);
  if (!high || !low) {
    state.SkipWithError("failed to create priority roundtrip channels");
    return;
  }

  PriorityPublisher publisher(high->publisher(), low->publisher());
  PrioritySubscriber subscriber(high->subscriber(), low->subscriber());
  SmallPayload payload{};
  std::array<std::uint32_t, 2> sequence{};
  std::uint64_t delivered = 0;
  for (auto _ : state) {
    const std::size_t priority = delivered % 3 == 2 ? 1 : 0;
    payload.producer = static_cast<std::uint32_t>(priority);
    payload.seq = sequence[priority]++;
    auto offered = publisher.offer(priority, std::as_bytes(std::span{&payload, 1}));
    if (!offered) {
      state.SkipWithError("priority offer failed");
      return;
    }
    auto received = subscriber.try_recv_checked();
    if (!received || !*received || (**received).priority != priority ||
        decode_small((**received).message.payload).producer != priority) {
      state.SkipWithError("priority scheduling mismatch");
      return;
    }
    benchmark::DoNotOptimize((**received).message.payload.data());
    if (!subscriber.release(**received)) {
      state.SkipWithError("priority release failed");
      return;
    }
    ++delivered;
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(sizeof(payload)));
}

}  // namespace

// 注册各 stress benchmark：SPSC 往返以微秒级单位度量，MPSC 多生产者吞吐以毫秒级度量。
BENCHMARK(BM_fifo_roundtrip_64b)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_ttl_expiration_64b)->Unit(benchmark::kMicrosecond);
// 每个 FIFO 生产者发送 65536 条 64 字节消息，度量稳态吞吐(throughput)。
BENCHMARK_TEMPLATE(BM_four_producers_64b, salias::Mode::FifoMpsc)
    ->Arg(65536)
    ->Unit(benchmark::kMillisecond);
// Ordered 模式额外保证跨生产者全局有序，对比 FIFO 度量保序带来的开销。
BENCHMARK_TEMPLATE(BM_four_producers_64b, salias::Mode::OrderedMpsc)
    ->Arg(65536)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_TEMPLATE(BM_latest_fast_subscriber_64b, 50)->Unit(benchmark::kNanosecond);
BENCHMARK_TEMPLATE(BM_latest_fast_subscriber_64b, 90)->Unit(benchmark::kNanosecond);
BENCHMARK_TEMPLATE(BM_fanout_registration_roundtrip_64b, false)->Unit(benchmark::kNanosecond);
BENCHMARK_TEMPLATE(BM_fanout_registration_roundtrip_64b, true)->Unit(benchmark::kNanosecond);
BENCHMARK_TEMPLATE(BM_latest_policy_roundtrip_64b, salias::SubscriptionPolicy::DropOldest)
    ->Unit(benchmark::kNanosecond);
BENCHMARK_TEMPLATE(BM_latest_policy_roundtrip_64b, salias::SubscriptionPolicy::Reliable)
    ->Unit(benchmark::kNanosecond);
BENCHMARK_TEMPLATE(BM_latest_slow_subscriber_64b, 50)->Arg(4096)->Unit(benchmark::kMicrosecond);
BENCHMARK_TEMPLATE(BM_latest_slow_subscriber_64b, 90)->Arg(4096)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_latest_message_window_64b)->Arg(4096)->Unit(benchmark::kMicrosecond);
BENCHMARK_TEMPLATE(BM_latest_concurrent_fanout_64b, 50)
    ->Arg(65536)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_latest_concurrent_fanout_64b, 90)
    ->Arg(65536)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_latest_checked_roundtrip_64b)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_latest_seek_64b)->Arg(4096)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_keyed_latest_roundtrip_64b)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_priority_weighted_roundtrip_64b)->Unit(benchmark::kMicrosecond);

// Google Benchmark 主入口：解析命令行参数并运行上述所有已注册 benchmark。
BENCHMARK_MAIN();
