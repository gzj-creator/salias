/**
 * @file test/channel/hybrid_mpsc_channel_test.cpp
 * @brief 进程内混合 MPSC channel 的 GoogleTest 单元测试。
 * @details 本文件位于测试层，验证 L5 channel(HybridMpscChannel) 的核心契约：
 *          每生产者独立 ring 容量、Ordered/Fifo 两种消费序、流控窗口背压、
 *          批量 claim/poll、消费进度 flush 与环绕(wraparound)安全。
 *          上游依赖：L2 frame(sequence 序列号低位编码、codec 帧对齐)、
 *          L3 flow(producer claim/commit、consumer poll/try_recv)、
 *          L4 等待策略(经模板注入，此处用 NoWakeWait 模拟忙轮询策略)。
 *          线程模型：本测试为单线程串行调用 producer/consumer 句柄，
 *          覆盖原子内存序(release/acquire)在单线程下的正确性，不验证并发竞态。
 */

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
/// 匿名命名空间：本测试文件内部使用的辅助类型、编解码工具与各 TEST 用例。

using salias::channel::HybridMpscChannel;
using salias::channel::Order;
using salias::flow::FlowError;

/// 测试用负载：携带生产者编号与序号，便于在消费端校验来源与顺序。
struct Payload {
  std::uint32_t producer = 0;
  std::uint32_t seq = 0;
};

/// 测试用等待策略桩：needs_wake 恒为 false，模拟忙轮询策略，记录 wake 调用次数。
struct NoWakeWait {
  static inline int wake_count = 0;

  /// 忙轮询策略声明永不需唤醒，channel 因此在 commit 时跳过 wake 调用。
  constexpr bool needs_wake() const noexcept { return false; }
  /// 空实现：忙轮询不阻塞。
  void wait(std::uint32_t*, std::uint32_t) noexcept {}
  /// 虽被调用则计数，用以验证"needs_wake=false 时 commit 不会触发 wake"。
  void wake(std::uint32_t*) noexcept { ++wake_count; }
  /// 空实现：忙轮询无需重置状态。
  void reset() noexcept {}
};

/// 将 payload 内存原样拷出，还原为测试负载结构。
Payload decode(std::span<const std::byte> payload) {
  Payload value{};
  std::memcpy(&value, payload.data(), sizeof(value));
  return value;
}

/// 将测试负载写入目的字节缓冲；模拟生产者填充 payload。
void write_payload(std::span<std::byte> dst, const Payload& payload) {
  std::memcpy(dst.data(), &payload, sizeof(payload));
}

/// 向批量 claim 的第 index 帧写入负载：定位到帧头之后的 payload 区域再写入。
/// @param batch 批量 claim 句柄，含连续 region 与帧长度。
/// @param index 帧在批次内的索引(0-based)。
/// @param payload 待写入的测试负载。
void write_batch_payload(salias::flow::BatchClaim& batch, std::uint32_t index,
                         const Payload& payload) {
  // 第 index 帧的起始偏移 = index * 单帧长度；跳过帧头(kHeaderSize)定位 payload。
  auto dst = batch.region.subspan(
      static_cast<std::size_t>(index) * batch.frame_len + salias::frame::kHeaderSize,
      sizeof(payload));
  write_payload(dst, payload);
}

/// 验证 sequence 序列号低 24 位入帧头 metadata、高 40 位入旁路存储的拆分重建。
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

/// 每个 producer 拥有独立 ring 容量：一个填满不影响另一个的 claim。
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

  // 第一个环已满：再 claim 应回压(BackPressured)，但不影响第二个环。
  auto blocked = first_tx.claim(sizeof(Payload));
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), FlowError::BackPressured);

  // 第二个 producer 独立 ring 仍有空间；全局 sequence 跨环单调递增。
  auto second = second_tx.claim(4088);
  ASSERT_TRUE(second);
  EXPECT_EQ(second->sequence, 1u);
}

/// 忙轮询策略(needs_wake=false)在 commit 时不应触发 wake，避免无谓开销。
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

  // commit 因 needs_wake=false 跳过 wake，计数应保持 0。
  EXPECT_EQ(NoWakeWait::wake_count, 0);
}

/// 环容量过大导致序列号低位可能别名(wraparound 歧义)时，建表应拒绝并返回 BadConfig。
TEST(HybridMpscChannelTest, RejectsWindowThatCanAliasLowSequenceBits) {
  auto created = HybridMpscChannel<>::create(HybridMpscChannel<>::Config{
      .num_producers = 2,
      .ring_capacity_per_producer = 128u * 1024u * 1024u,
  });

  // 128MB/帧远超 24 位低位窗口安全范围，静态校验失败 -> BadConfig。
  ASSERT_FALSE(created);
  EXPECT_EQ(created.error(), salias::channel::ChannelError::BadConfig);
}

/// Ordered 全序：即便后序帧先 commit，consumer 也必须等到前序帧可见后才按序消费。
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

  // 先 commit 序列号较大的帧(sequence=1)。
  write_payload(second->payload, Payload{.producer = 1, .seq = 20});
  second_tx.commit(second.value());

  // 全序模式：前序帧(sequence=0)未提交，consumer 不能"跳过"消费。
  EXPECT_FALSE(rx.try_recv().has_value());

  // 再 commit 前序帧，consumer 立即可见并按全局 sequence 顺序交付。
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

/// Fifo 模式：consumer 按各环就绪顺序消费，不强制全局序，先 commit 的帧即可被取走。
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
  // Fifo 模式下每个环各自从 sequence=0 开始(环内 FIFO)。
  EXPECT_EQ(first->sequence, 0u);
  EXPECT_EQ(second->sequence, 0u);

  write_payload(second->payload, Payload{.producer = 1, .seq = 20});
  second_tx.commit(second.value());

  // 无全局序约束：先 commit 的 producer=1 帧立即可消费。
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

/// Fifo 扇出：多 consumer 各持独立游标，未全部 release 前帧空间不可回收(背压)。
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

  // 两个 consumer 各自都能看到同一帧(独立游标)。
  auto first_seen = first_rx.try_recv();
  auto second_seen = second_rx.try_recv();
  ASSERT_TRUE(first_seen);
  ASSERT_TRUE(second_seen);
  EXPECT_EQ(decode(first_seen->payload).seq, 7u);
  EXPECT_EQ(decode(second_seen->payload).seq, 7u);

  // 仅 release 一个 consumer：最慢游标未推进，帧空间不可回收 -> 回压。
  first_rx.release(*first_seen);
  auto blocked = tx.claim(sizeof(Payload));
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), FlowError::BackPressured);

  // 全部 consumer release 后空间释放，生产者可继续 claim。
  second_rx.release(*second_seen);
  EXPECT_TRUE(tx.claim(sizeof(Payload)));
  EXPECT_FALSE(first_rx.try_recv().has_value());
  EXPECT_FALSE(second_rx.try_recv().has_value());
}

/// Ordered 扇出：每个 consumer 独立维护全序视图，各自按全局 sequence 顺序交付。
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
  // 先 commit 后序帧：全序下两 consumer 都不可见(缺前序)。
  second_tx.commit(second.value());
  EXPECT_FALSE(first_rx.try_recv().has_value());
  EXPECT_FALSE(second_rx.try_recv().has_value());
  first_tx.commit(first.value());

  // 前序到位后，两个 consumer 各自按 sequence=0 -> 1 顺序交付。
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

/// 批量 claim 分配连续全局 sequence，一次性 commit 后 poll 按序收全。
TEST(HybridMpscChannelTest, BatchClaimAllocatesContiguousGlobalSequences) {
  auto created = HybridMpscChannel<>::create(HybridMpscChannel<>::Config{
      .num_producers = 1,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx(0);
  auto rx = channel.rx();

  // 一次申请 3 帧：base_sequence 起始为 0，frame_len 含帧头对齐。
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

/// 流控窗口(publication_window)把生产者在途字节数限制在窗口内，先于环容量触发回压。
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

/// publication_window=0 时退化为传统整环背压行为(零回归)。
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

/// 批量 poll 内部一次性 flush 消费进度，使生产者立即回收整窗空间。
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

/// 消费者缓存的 visible_producer_pos 跨多次 poll 逐步刷新，分批 commit 不丢消息。
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

/// 环绕(wraparound)后重新 claim 的帧不应让消费者看到上一轮残留的已提交帧头。
TEST(HybridMpscChannelTest, WrappedClaimDoesNotExposeStaleCommittedHeader) {
  using FifoChannel = HybridMpscChannel<Order::Fifo>;
  auto created = FifoChannel::create(FifoChannel::Config{
      .num_producers = 1,
      .ring_capacity_per_producer = 4096,
  });
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx(0);
  auto rx = channel.rx();

  auto first = tx.claim(4088);
  ASSERT_TRUE(first);
  tx.commit(first.value());

  auto consumed = rx.try_recv();
  ASSERT_TRUE(consumed.has_value());
  rx.consume(*consumed);
  // flush_progress 把消费位置发布给生产者侧，使已消费帧可被环绕复用。
  rx.flush_progress();

  // 环绕复用同一物理槽位重新 claim，但尚未 commit，消费者不应读到旧帧头。
  auto wrapped = tx.claim(4088);
  ASSERT_TRUE(wrapped);
  EXPECT_FALSE(rx.try_recv().has_value());
}

/// try_recv_run 从单个 ring 连续排空多条就绪帧，保证环内顺序。
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

/// try_recv_run 的 cap 小于可读帧数时精确截断，剩余帧在下次调用取回。
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

/// 批量 poll 交付的逐帧内容与全局序一致，消费后空间完整回收。
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
