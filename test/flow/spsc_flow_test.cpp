/** @file test/flow/spsc_flow_test.cpp
 * @brief salias L3 flow 层 SPSC（单生产者单消费者）流的往返与流控单元测试。
 * @details 本文件位于 L3 flow（生产/消费位置与流控）层，在 L1 ring 与 L2 frame
 *   基础之上验证 Producer/Consumer 的 claim→commit→poll→advance 往返闭环，
 *   以及流控窗口（flow window）背压机制：当 ring 容量耗尽时 producer.claim
 *   返回 BackPressured，消费者 advance 释放窗口后背压立即解除。关键不变式：
 *   producer_pos 与 consumer_pos 均以“帧总长（frame_len）=帧头+对齐 payload”
 *   为步进单位；poll 仅在 producer_pos 超前 consumer_pos 时返回可见消息。
 */

#include "core/flow/consumer.hpp"
#include "core/flow/producer.hpp"
#include "core/frame/codec.hpp"
#include "core/platform/mapping.hpp"
#include "core/ring/magic_ring.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>

namespace {  // 匿名命名空间：SPSC flow 测试的 fixture 与用例，仅本编译单元可见。

/// @brief SPSC flow 测试夹具，聚合 ring 与生产/消费位置。
/// @details 持有一个 L1 ring（MagicRing）及其底层 L0 内存映射（Mapping），
///   外加 producer/consumer 两个位置计数器。位置以帧总长为单位单调递增，
///   flow 层 Producer/Consumer 通过 Positions 指针引用它们以实现流控窗口。
struct FlowFixture {
  salias::platform::Mapping mapping;
  salias::ring::MagicRing ring;
  std::uint64_t producer_pos = 0;
  std::uint64_t consumer_pos = 0;

  // 返回当前 fixture 的 flow 层位置指针。
  salias::flow::Positions positions() noexcept {
    return salias::flow::Positions{
        .producer = &producer_pos,
        .consumer = &consumer_pos,
        .cap = ring.capacity(),
    };
  }
};

// 为 SPSC flow 测试创建页大小的 magic ring fixture。
FlowFixture make_fixture() {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  EXPECT_GT(raw_page_size, 0);

  auto mapping = salias::platform::Mapping::create(
      salias::platform::MapOptions{.size = static_cast<std::size_t>(raw_page_size)});
  EXPECT_TRUE(mapping);
  auto ring = salias::ring::MagicRing::create(std::move(mapping).value());
  EXPECT_TRUE(ring);

  return FlowFixture{.mapping = salias::platform::Mapping{},
                     .ring = std::move(ring).value(),
                     .producer_pos = 0,
                     .consumer_pos = 0};
}

// 验证 SPSC claim/commit/poll/release 往返路径。
TEST(SpscFlowTest, ClaimCommitPollAndAdvanceRoundTripsPayload) {
  FlowFixture fixture = make_fixture();
  salias::flow::Producer producer(fixture.ring, fixture.positions());
  salias::flow::Consumer consumer(fixture.ring, fixture.positions());

  constexpr std::array payload{
      std::byte{0x41}, std::byte{0x42}, std::byte{0x43}, std::byte{0x44},
      std::byte{0x45}, std::byte{0x46}, std::byte{0x47}, std::byte{0x48},
      std::byte{0x49},
  };

  auto claim = producer.claim(payload.size());
  ASSERT_TRUE(claim);
  ASSERT_EQ(claim.value().payload.size(), payload.size());
  std::memcpy(claim.value().payload.data(), payload.data(), payload.size());

  producer.commit(claim.value());

  // producer_pos 应推进一个帧总长（帧头 + 对齐后的 payload）。
  EXPECT_EQ(fixture.producer_pos, salias::frame::frame_len(payload.size()));
  auto message = consumer.poll();
  ASSERT_TRUE(message.has_value());
  ASSERT_EQ(message->payload.size(), payload.size());
  for (std::size_t i = 0; i < payload.size(); ++i) {
    EXPECT_EQ(message->payload[i], payload[i]) << "payload byte " << i;
  }

  consumer.advance(message->next_position);
  // 消费后 consumer_pos 追上 producer_pos，ring 为空，再次 poll 无消息。
  EXPECT_EQ(fixture.consumer_pos, fixture.producer_pos);
  EXPECT_FALSE(consumer.poll().has_value());
}

// 验证释放消费者进度后生产者背压会解除。
TEST(SpscFlowTest, BackPressureClearsImmediatelyAfterAdvance) {
  FlowFixture fixture = make_fixture();
  salias::flow::Producer producer(fixture.ring, fixture.positions());
  salias::flow::Consumer consumer(fixture.ring, fixture.positions());

  // 构造恰好填满 ring 可用空间的 payload（capacity 减去帧头大小）。
  const std::uint32_t payload_len =
      static_cast<std::uint32_t>(fixture.ring.capacity() - salias::frame::kHeaderSize);
  auto claim = producer.claim(payload_len);
  ASSERT_TRUE(claim);
  producer.commit(claim.value());

  // ring 已满，再 claim 必然触发流控窗口背压。
  auto blocked = producer.claim(1);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::flow::FlowError::BackPressured);

  auto message = consumer.poll();
  ASSERT_TRUE(message);
  // 消费者推进游标后释放流控窗口，背压随之解除。
  consumer.advance(message->next_position);

  // 窗口已释放，producer 可重新 claim。
  auto unblocked = producer.claim(1);
  ASSERT_TRUE(unblocked);
}

// 验证超过单个 ring 容量的 payload 会被拒绝。
TEST(SpscFlowTest, RejectsPayloadLargerThanSingleRingCapacity) {
  FlowFixture fixture = make_fixture();
  salias::flow::Producer producer(fixture.ring, fixture.positions());

  // 请求整个 capacity（无帧头余量）必然超出单帧上限。
  auto claim = producer.claim(static_cast<std::uint32_t>(fixture.ring.capacity()));

  ASSERT_FALSE(claim);
  EXPECT_EQ(claim.error(), salias::flow::FlowError::MessageTooLarge);
}

}  // namespace
