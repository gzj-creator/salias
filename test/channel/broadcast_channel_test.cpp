#include "core/channel/broadcast.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

namespace {

using salias::channel::BroadcastChannel;
using salias::channel::ChannelConfig;
using salias::flow::FlowError;

// 解码测试生成的小端 uint32 payload。
std::uint32_t decode_u32(std::span<const std::byte> payload) {
  std::uint32_t value = 0;
  std::memcpy(&value, payload.data(), sizeof(value));
  return value;
}

// 验证每个 broadcast 订阅者都能独立收到每条消息。
TEST(BroadcastChannelTest, EachSubscriberReceivesEveryMessageWithIndependentProgress) {
  auto created = BroadcastChannel<>::create(ChannelConfig{.capacity = 4096});
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto first = channel.subscribe();
  auto second = channel.subscribe();
  auto tx = channel.tx();

  constexpr std::array<std::uint32_t, 3> values{11, 22, 33};
  for (const auto value : values) {
    auto offered = tx.offer(std::as_bytes(std::span{&value, 1}));
    ASSERT_TRUE(offered);
  }

  for (const auto expected : values) {
    auto message = first.try_recv();
    ASSERT_TRUE(message);
    EXPECT_EQ(decode_u32(message->payload), expected);
    first.release(*message);
  }

  for (const auto expected : values) {
    auto message = second.try_recv();
    ASSERT_TRUE(message);
    EXPECT_EQ(decode_u32(message->payload), expected);
    second.release(*message);
  }

  EXPECT_FALSE(first.try_recv());
  EXPECT_FALSE(second.try_recv());
}

// 验证 broadcast 背压由最慢订阅者控制。
TEST(BroadcastChannelTest, ReliableModeBackpressuresUntilSlowestSubscriberReleasesSpace) {
  auto created = BroadcastChannel<>::create(ChannelConfig{.capacity = 4096});
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto slow = channel.subscribe();
  auto fast = channel.subscribe();
  auto tx = channel.tx();

  std::vector<std::byte> payload(1000, std::byte{0x42});
  for (int i = 0; i < 4; ++i) {
    auto offered = tx.offer(payload);
    ASSERT_TRUE(offered);
  }

  for (int i = 0; i < 4; ++i) {
    auto message = fast.try_recv();
    ASSERT_TRUE(message);
    fast.release(*message);
  }

  auto blocked = tx.offer(payload);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), FlowError::BackPressured);

  auto released = slow.try_recv();
  ASSERT_TRUE(released);
  slow.release(*released);

  auto unblocked = tx.offer(payload);
  ASSERT_TRUE(unblocked);
}

}  // namespace
