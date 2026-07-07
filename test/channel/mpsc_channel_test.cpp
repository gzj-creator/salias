#include "core/channel/mpsc.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <thread>
#include <vector>

namespace {

using salias::channel::ChannelConfig;
using salias::channel::MpscChannel;
using salias::flow::FlowError;

struct Payload {
  std::uint32_t producer = 0;
  std::uint32_t seq = 0;
};

// 从接收到的字节 span 解码测试 payload。
Payload decode(std::span<const std::byte> payload) {
  Payload value{};
  std::memcpy(&value, payload.data(), sizeof(value));
  return value;
}

// 验证消费者不会跳过更早的未提交预留区间。
TEST(MpscChannelTest, DoesNotExposeCommittedFrameBehindUncommittedGap) {
  auto created = MpscChannel<>::create(ChannelConfig{.capacity = 4096});
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto first_tx = channel.tx();
  auto second_tx = channel.tx();
  auto rx = channel.rx();

  auto first = first_tx.claim(sizeof(Payload));
  ASSERT_TRUE(first);
  auto second = second_tx.claim(sizeof(Payload));
  ASSERT_TRUE(second);

  const Payload second_payload{.producer = 2, .seq = 1};
  std::memcpy(second.value().payload.data(), &second_payload, sizeof(second_payload));
  second_tx.commit(second.value());

  EXPECT_FALSE(rx.try_recv().has_value());

  const Payload first_payload{.producer = 1, .seq = 1};
  std::memcpy(first.value().payload.data(), &first_payload, sizeof(first_payload));
  first_tx.commit(first.value());

  auto first_seen = rx.try_recv();
  ASSERT_TRUE(first_seen);
  EXPECT_EQ(decode(first_seen->payload).producer, 1u);
  rx.release(*first_seen);

  auto second_seen = rx.try_recv();
  ASSERT_TRUE(second_seen);
  EXPECT_EQ(decode(second_seen->payload).producer, 2u);
  rx.release(*second_seen);
}

// 验证多个生产者线程会发布唯一且有序的序列。
TEST(MpscChannelTest, MultipleProducersPublishUniqueSequences) {
  constexpr std::uint32_t kProducerCount = 4;
  constexpr std::uint32_t kMessagesPerProducer = 64;
  constexpr std::uint32_t kTotalMessages = kProducerCount * kMessagesPerProducer;

  auto created = MpscChannel<>::create(ChannelConfig{.capacity = 1u << 16});
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  std::atomic<bool> start{false};
  std::vector<std::thread> producers;
  producers.reserve(kProducerCount);

  for (std::uint32_t producer = 0; producer < kProducerCount; ++producer) {
    producers.emplace_back([producer, &channel, &start] {
      auto tx = channel.tx();
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }

      for (std::uint32_t seq = 0; seq < kMessagesPerProducer; ++seq) {
        const Payload payload{.producer = producer, .seq = seq};
        for (;;) {
          auto offered = tx.offer(std::as_bytes(std::span{&payload, 1}));
          if (offered && offered.value()) {
            break;
          }
          ASSERT_FALSE(offered.error() == FlowError::MessageTooLarge);
          std::this_thread::yield();
        }
      }
    });
  }

  auto rx = channel.rx();
  std::array<std::bitset<kMessagesPerProducer>, kProducerCount> seen{};
  start.store(true, std::memory_order_release);

  std::uint32_t received = 0;
  while (received < kTotalMessages) {
    auto message = rx.try_recv();
    if (!message.has_value()) {
      std::this_thread::yield();
      continue;
    }

    const Payload payload = decode(message->payload);
    ASSERT_LT(payload.producer, kProducerCount);
    ASSERT_LT(payload.seq, kMessagesPerProducer);
    EXPECT_FALSE(seen[payload.producer].test(payload.seq));
    seen[payload.producer].set(payload.seq);
    ++received;
    rx.release(*message);
  }

  for (auto& producer : producers) {
    producer.join();
  }

  for (std::uint32_t producer = 0; producer < kProducerCount; ++producer) {
    EXPECT_EQ(seen[producer].count(), kMessagesPerProducer);
  }
}

}  // namespace
