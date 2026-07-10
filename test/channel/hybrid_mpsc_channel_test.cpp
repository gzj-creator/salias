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

TEST(HybridMpscChannelTest, PublicationWindowLimitsInflightBytesBelowRingCapacity) {
  // 环容量 4096 但窗口 256 字节；生产者最多领先消费者 256/frame_len 帧即回压，而非填满整环。
  constexpr std::size_t kWindow = 256;
  const std::size_t frame_len = salias::frame::frame_len(sizeof(Payload));
  const std::uint32_t expected_frames = static_cast<std::uint32_t>(kWindow / frame_len);

  auto created = HybridMpscChannel<>::create(HybridMpscChannel<>::Config{
      .num_producers = 1,
      .ring_capacity_per_producer = 4096,
      .publication_window = kWindow,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx(0);

  std::uint32_t claimed = 0;
  for (;;) {
    auto claim = tx.claim(sizeof(Payload));
    if (!claim) {
      EXPECT_EQ(claim.error(), FlowError::BackPressured);
      break;
    }
    write_payload(claim->payload, Payload{.producer = 0, .seq = claimed});
    tx.commit(claim.value());
    ++claimed;
    ASSERT_LT(claimed, 4096u / frame_len) << "window failed to bound inflight below ring capacity";
  }
  EXPECT_EQ(claimed, expected_frames);

  // 消费一部分后窗口重新打开，生产者可继续发布。
  auto rx = channel.rx();
  auto first = rx.try_recv();
  ASSERT_TRUE(first);
  rx.release(*first);
  EXPECT_TRUE(tx.claim(sizeof(Payload)));
}

TEST(HybridMpscChannelTest, ZeroWindowKeepsFullRingBehavior) {
  // 默认 publication_window=0：生产者可填满整环，保持既有零回归行为。
  auto created = HybridMpscChannel<>::create(HybridMpscChannel<>::Config{
      .num_producers = 1,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx(0);
  auto claim = tx.claim(4088);  // 4088 payload -> frame_len == 4096 填满整环
  ASSERT_TRUE(claim);
  tx.commit(claim.value());

  auto blocked = tx.claim(sizeof(Payload));
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), FlowError::BackPressured);
}

TEST(HybridMpscChannelTest, BatchedPollFlushesProgressAndReclaimsSpace) {
  // poll 内部用 consume()+flush_progress() 批量提交进度；一次 poll 后生产者应能回收全部空间。
  constexpr std::size_t kWindow = 256;
  auto created = HybridMpscChannel<>::create(HybridMpscChannel<>::Config{
      .num_producers = 1,
      .ring_capacity_per_producer = 4096,
      .publication_window = kWindow,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx(0);
  auto rx = channel.rx();

  const std::size_t frame_len = salias::frame::frame_len(sizeof(Payload));
  const std::uint32_t window_frames = static_cast<std::uint32_t>(kWindow / frame_len);

  // 填满窗口。
  for (std::uint32_t i = 0; i < window_frames; ++i) {
    auto claim = tx.claim(sizeof(Payload));
    ASSERT_TRUE(claim);
    write_payload(claim->payload, Payload{.producer = 0, .seq = i});
    tx.commit(claim.value());
  }
  ASSERT_FALSE(tx.claim(sizeof(Payload)));  // 窗口已满

  // 一次 poll 排空并批量 flush 进度。
  std::vector<std::uint32_t> seen;
  const std::uint32_t drained = rx.poll(
      [&seen](std::span<const std::byte> payload) { seen.push_back(decode(payload).seq); },
      window_frames);
  EXPECT_EQ(drained, window_frames);
  ASSERT_EQ(seen.size(), window_frames);
  for (std::uint32_t i = 0; i < window_frames; ++i) {
    EXPECT_EQ(seen[i], i);
  }

  // flush 已把消费进度发布给生产者，整窗空间应可复用。
  for (std::uint32_t i = 0; i < window_frames; ++i) {
    EXPECT_TRUE(tx.claim(sizeof(Payload))) << "space not reclaimed after batched flush at " << i;
    // 立即 commit 以推进 producer_pos，便于继续填充。
  }
}

TEST(HybridMpscChannelTest, CachedVisiblePositionSeesStagedCommitsAcrossPolls) {
  // 消费者缓存 visible_producer_pos；分批 commit 时多次 poll 必须收全，不丢消息。
  auto created = HybridMpscChannel<>::create(HybridMpscChannel<>::Config{
      .num_producers = 1,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx(0);
  auto rx = channel.rx();

  auto publish = [&](std::uint32_t seq) {
    auto claim = tx.claim(sizeof(Payload));
    ASSERT_TRUE(claim);
    write_payload(claim->payload, Payload{.producer = 0, .seq = seq});
    tx.commit(claim.value());
  };

  std::vector<std::uint32_t> seen;
  auto drain = [&] {
    return rx.poll([&seen](std::span<const std::byte> payload) { seen.push_back(decode(payload).seq); },
                   64);
  };

  publish(0);
  publish(1);
  EXPECT_EQ(drain(), 2u);  // 第一次 poll 刷新缓存并收到 2 条

  publish(2);
  publish(3);
  publish(4);
  EXPECT_EQ(drain(), 3u);  // 缓存追平后再次刷新，收到新提交的 3 条

  EXPECT_EQ(seen, (std::vector<std::uint32_t>{0, 1, 2, 3, 4}));
  EXPECT_FALSE(rx.try_recv().has_value());
}

TEST(HybridMpscChannelTest, TryRecvRunDrainsContiguousFramesFromSingleRing) {
  // FIFO 单生产者环：try_recv_run 应在同一环内连续排空多条，顺序正确。
  using FifoChannel = HybridMpscChannel<Order::Fifo>;
  auto created = FifoChannel::create(FifoChannel::Config{
      .num_producers = 1,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx(0);
  auto rx = channel.rx();

  constexpr std::uint32_t kCount = 5;
  for (std::uint32_t i = 0; i < kCount; ++i) {
    auto claim = tx.claim(sizeof(Payload));
    ASSERT_TRUE(claim);
    write_payload(claim->payload, Payload{.producer = 0, .seq = i});
    tx.commit(claim.value());
  }

  std::array<salias::flow::Message, 8> buffer;
  const std::uint32_t got = rx.try_recv_run(buffer.data(), static_cast<std::uint32_t>(buffer.size()));
  EXPECT_EQ(got, kCount);
  for (std::uint32_t i = 0; i < got; ++i) {
    EXPECT_EQ(decode(buffer[i].payload).seq, i);
  }
  rx.flush_progress();
  EXPECT_FALSE(rx.try_recv().has_value());
}

TEST(HybridMpscChannelTest, TryRecvRunTruncatesAtCap) {
  // cap 小于可读条数时应精确截断，剩余下次取回。
  using FifoChannel = HybridMpscChannel<Order::Fifo>;
  auto created = FifoChannel::create(FifoChannel::Config{
      .num_producers = 1,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx(0);
  auto rx = channel.rx();

  for (std::uint32_t i = 0; i < 5; ++i) {
    auto claim = tx.claim(sizeof(Payload));
    ASSERT_TRUE(claim);
    write_payload(claim->payload, Payload{.producer = 0, .seq = i});
    tx.commit(claim.value());
  }

  std::array<salias::flow::Message, 8> buffer;
  const std::uint32_t first = rx.try_recv_run(buffer.data(), 2);
  EXPECT_EQ(first, 2u);
  EXPECT_EQ(decode(buffer[0].payload).seq, 0u);
  EXPECT_EQ(decode(buffer[1].payload).seq, 1u);
  rx.flush_progress();

  const std::uint32_t second = rx.try_recv_run(buffer.data(), 8);
  EXPECT_EQ(second, 3u);
  EXPECT_EQ(decode(buffer[0].payload).seq, 2u);
  EXPECT_EQ(decode(buffer[2].payload).seq, 4u);
  rx.flush_progress();
  EXPECT_EQ(rx.try_recv_run(buffer.data(), 8), 0u);
}

TEST(HybridMpscChannelTest, BatchedRunPollMatchesPerMessageContent) {
  // 批量内联 poll 的交付内容与逐条一致，且完整回收空间。
  auto created = HybridMpscChannel<>::create(HybridMpscChannel<>::Config{
      .num_producers = 2,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto first_tx = channel.tx(0);
  auto second_tx = channel.tx(1);
  auto rx = channel.rx();

  // Ordered 全序：交替提交两个生产者，消费者按 global sequence 收取。
  auto first = first_tx.claim(sizeof(Payload));
  auto second = second_tx.claim(sizeof(Payload));
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  write_payload(first->payload, Payload{.producer = 0, .seq = 100});
  write_payload(second->payload, Payload{.producer = 1, .seq = 200});
  first_tx.commit(first.value());
  second_tx.commit(second.value());

  std::vector<std::uint32_t> seen;
  const std::uint32_t count =
      rx.poll([&seen](const salias::flow::Message& message) { seen.push_back(decode(message.payload).seq); },
              8);
  EXPECT_EQ(count, 2u);
  EXPECT_EQ(seen, (std::vector<std::uint32_t>{100, 200}));
  EXPECT_FALSE(rx.try_recv().has_value());
}

}  // namespace
