#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

#include "core/channel/hybrid_mpsc.hpp"
#include "core/frame/codec.hpp"
#include "core/frame/sequence.hpp"

namespace {

using salias::channel::HybridMpscChannel;
using salias::channel::Order;
using salias::flow::FlowError;

struct Payload {
  std::uint32_t producer = 0;
  std::uint32_t seq = 0;
};

struct NoWakeWait {
  static inline int wake_count = 0;

  constexpr bool needs_wake() const noexcept { return false; }
  void wait(std::uint32_t*, std::uint32_t) noexcept {}
  void wake(std::uint32_t*) noexcept { ++wake_count; }
  void reset() noexcept {}
};

Payload decode(std::span<const std::byte> payload) {
  Payload value{};
  std::memcpy(&value, payload.data(), sizeof(value));
  return value;
}

void write_payload(std::span<std::byte> dst, const Payload& payload) {
  std::memcpy(dst.data(), &payload, sizeof(payload));
}

void write_batch_payload(salias::flow::BatchClaim& batch, std::uint32_t index,
                         const Payload& payload) {
  auto dst = batch.region.subspan(
      static_cast<std::size_t>(index) * batch.frame_len + salias::frame::kHeaderSize,
      sizeof(payload));
  write_payload(dst, payload);
}

TEST(FrameSequenceTest, EncodesLowSequenceBitsAndPreservesFlags) {
  constexpr std::uint64_t kSequence = 0x1234'5678'9ABCull;
  constexpr std::uint32_t kFlags = salias::frame::FLAG_BEGIN | salias::frame::FLAG_END;

  std::uint32_t meta = kFlags;
  std::uint64_t sequence_high = 0;

  salias::frame::encode_sequence(kSequence, meta, sequence_high);

  EXPECT_EQ(meta & 0xFFu, kFlags);
  EXPECT_EQ((meta >> 8) & 0x00FF'FFFFu, 0x0078'9ABCu);
  EXPECT_EQ(sequence_high, kSequence >> 24);
  EXPECT_EQ(salias::frame::decode_sequence(meta, sequence_high), kSequence);
}

TEST(HybridMpscChannelTest, ProducersHaveIndependentRingCapacity) {
  auto created = HybridMpscChannel<>::create(HybridMpscChannel<>::Config{
      .num_producers = 2,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  EXPECT_EQ(channel.producer_count(), 2u);
  EXPECT_EQ(channel.ring_capacity_per_producer(), 4096u);

  auto first_tx = channel.tx(0);
  auto second_tx = channel.tx(1);

  auto first = first_tx.claim(4088);
  ASSERT_TRUE(first);
  EXPECT_EQ(first->sequence, 0u);

  auto blocked = first_tx.claim(sizeof(Payload));
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), FlowError::BackPressured);

  auto second = second_tx.claim(4088);
  ASSERT_TRUE(second);
  EXPECT_EQ(second->sequence, 1u);
}

TEST(HybridMpscChannelTest, CommitSkipsWakeForBusyPollingStrategy) {
  using Channel = HybridMpscChannel<Order::Ordered, NoWakeWait>;
  auto created = Channel::create(Channel::Config{
      .num_producers = 1,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  NoWakeWait::wake_count = 0;
  auto channel = std::move(created).value();
  auto tx = channel.tx(0);
  auto claim = tx.claim(sizeof(Payload));
  ASSERT_TRUE(claim);
  tx.commit(claim.value());

  EXPECT_EQ(NoWakeWait::wake_count, 0);
}

TEST(HybridMpscChannelTest, RejectsWindowThatCanAliasLowSequenceBits) {
  auto created = HybridMpscChannel<>::create(HybridMpscChannel<>::Config{
      .num_producers = 2,
      .ring_capacity_per_producer = 128u * 1024u * 1024u,
  });

  ASSERT_FALSE(created);
  EXPECT_EQ(created.error(), salias::channel::ChannelError::BadConfig);
}

TEST(HybridMpscChannelTest, ConsumerWaitsForEarlierSequenceEvenWhenLaterCommitArrivesFirst) {
  auto created = HybridMpscChannel<>::create(HybridMpscChannel<>::Config{
      .num_producers = 2,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto first_tx = channel.tx(0);
  auto second_tx = channel.tx(1);
  auto rx = channel.rx();

  auto first = first_tx.claim(sizeof(Payload));
  ASSERT_TRUE(first);
  EXPECT_EQ(first->sequence, 0u);

  auto second = second_tx.claim(sizeof(Payload));
  ASSERT_TRUE(second);
  EXPECT_EQ(second->sequence, 1u);

  write_payload(second->payload, Payload{.producer = 1, .seq = 20});
  second_tx.commit(second.value());

  EXPECT_FALSE(rx.try_recv().has_value());

  write_payload(first->payload, Payload{.producer = 0, .seq = 10});
  first_tx.commit(first.value());

  auto first_seen = rx.try_recv();
  ASSERT_TRUE(first_seen);
  EXPECT_EQ(first_seen->sequence, 0u);
  EXPECT_EQ(first_seen->producer_id, 0u);
  EXPECT_EQ(decode(first_seen->payload).seq, 10u);
  rx.release(*first_seen);

  auto second_seen = rx.try_recv();
  ASSERT_TRUE(second_seen);
  EXPECT_EQ(second_seen->sequence, 1u);
  EXPECT_EQ(second_seen->producer_id, 1u);
  EXPECT_EQ(decode(second_seen->payload).seq, 20u);
  rx.release(*second_seen);
}

TEST(HybridMpscChannelTest, FifoConsumesReadyProducersWithoutGlobalOrdering) {
  using FifoChannel = HybridMpscChannel<Order::Fifo>;
  auto created = FifoChannel::create(FifoChannel::Config{
      .num_producers = 2,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto first_tx = channel.tx(0);
  auto second_tx = channel.tx(1);
  auto rx = channel.rx();

  auto first = first_tx.claim(sizeof(Payload));
  auto second = second_tx.claim(sizeof(Payload));
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  EXPECT_EQ(first->sequence, 0u);
  EXPECT_EQ(second->sequence, 0u);

  write_payload(second->payload, Payload{.producer = 1, .seq = 20});
  second_tx.commit(second.value());

  auto second_seen = rx.try_recv();
  ASSERT_TRUE(second_seen);
  EXPECT_EQ(second_seen->producer_id, 1u);
  EXPECT_EQ(decode(second_seen->payload).seq, 20u);
  rx.release(*second_seen);

  write_payload(first->payload, Payload{.producer = 0, .seq = 10});
  first_tx.commit(first.value());

  auto first_seen = rx.try_recv();
  ASSERT_TRUE(first_seen);
  EXPECT_EQ(first_seen->producer_id, 0u);
  EXPECT_EQ(decode(first_seen->payload).seq, 10u);
  rx.release(*first_seen);
}

TEST(HybridMpscChannelTest, FifoFanoutConsumersHaveIndependentCursors) {
  using FifoChannel = HybridMpscChannel<Order::Fifo>;
  auto created = FifoChannel::create(FifoChannel::Config{
      .num_producers = 1,
      .num_consumers = 2,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx(0);
  auto first_rx = channel.rx(0);
  auto second_rx = channel.rx(1);

  auto claim = tx.claim(4088);
  ASSERT_TRUE(claim);
  write_payload(claim->payload, Payload{.producer = 0, .seq = 7});
  tx.commit(claim.value());

  auto first_seen = first_rx.try_recv();
  auto second_seen = second_rx.try_recv();
  ASSERT_TRUE(first_seen);
  ASSERT_TRUE(second_seen);
  EXPECT_EQ(decode(first_seen->payload).seq, 7u);
  EXPECT_EQ(decode(second_seen->payload).seq, 7u);

  first_rx.release(*first_seen);
  auto blocked = tx.claim(sizeof(Payload));
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), FlowError::BackPressured);

  second_rx.release(*second_seen);
  EXPECT_TRUE(tx.claim(sizeof(Payload)));
  EXPECT_FALSE(first_rx.try_recv().has_value());
  EXPECT_FALSE(second_rx.try_recv().has_value());
}

TEST(HybridMpscChannelTest, OrderedFanoutPreservesGlobalOrderForEachConsumer) {
  using OrderedChannel = HybridMpscChannel<Order::Ordered>;
  auto created = OrderedChannel::create(OrderedChannel::Config{
      .num_producers = 2,
      .num_consumers = 2,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto first_tx = channel.tx(0);
  auto second_tx = channel.tx(1);
  auto first_rx = channel.rx(0);
  auto second_rx = channel.rx(1);

  auto first = first_tx.claim(sizeof(Payload));
  auto second = second_tx.claim(sizeof(Payload));
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  second_tx.commit(second.value());
  EXPECT_FALSE(first_rx.try_recv().has_value());
  EXPECT_FALSE(second_rx.try_recv().has_value());
  first_tx.commit(first.value());

  auto first_a = first_rx.try_recv();
  auto first_b = second_rx.try_recv();
  ASSERT_TRUE(first_a);
  ASSERT_TRUE(first_b);
  EXPECT_EQ(first_a->sequence, 0u);
  EXPECT_EQ(first_b->sequence, 0u);
  first_rx.release(*first_a);
  second_rx.release(*first_b);

  auto second_a = first_rx.try_recv();
  auto second_b = second_rx.try_recv();
  ASSERT_TRUE(second_a);
  ASSERT_TRUE(second_b);
  EXPECT_EQ(second_a->sequence, 1u);
  EXPECT_EQ(second_b->sequence, 1u);
  first_rx.release(*second_a);
  second_rx.release(*second_b);
}

TEST(HybridMpscChannelTest, BatchClaimAllocatesContiguousGlobalSequences) {
  auto created = HybridMpscChannel<>::create(HybridMpscChannel<>::Config{
      .num_producers = 1,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx(0);
  auto rx = channel.rx();

  auto batch = tx.claim_batch(sizeof(Payload), 3);
  ASSERT_TRUE(batch);
  EXPECT_EQ(batch->base_sequence, 0u);
  EXPECT_EQ(batch->frame_count, 3u);
  EXPECT_EQ(batch->frame_len, salias::frame::frame_len(sizeof(Payload)));

  const std::array payloads{
      Payload{.producer = 0, .seq = 0},
      Payload{.producer = 0, .seq = 1},
      Payload{.producer = 0, .seq = 2},
  };
  for (std::uint32_t i = 0; i < payloads.size(); ++i) {
    write_batch_payload(batch.value(), i, payloads[i]);
  }
  tx.commit_batch(batch.value());

  std::vector<std::uint32_t> seen;
  const std::uint32_t count = rx.poll(
      [&seen](std::span<const std::byte> payload) { seen.push_back(decode(payload).seq); }, 8);

  EXPECT_EQ(count, 3u);
  EXPECT_EQ(seen, (std::vector<std::uint32_t>{0, 1, 2}));
  EXPECT_FALSE(rx.try_recv().has_value());
}

}  // namespace
