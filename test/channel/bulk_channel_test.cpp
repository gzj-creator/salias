#include "core/channel/bulk.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <numeric>
#include <vector>

namespace {

using salias::channel::BulkChannel;
using salias::channel::ChannelConfig;
using salias::flow::FlowError;

// 验证大 bulk payload 会作为一个连续帧被接收。
TEST(BulkChannelTest, LargeMessageIsReceivedAsOneContiguousFrame) {
  auto created = BulkChannel<>::create(ChannelConfig{.capacity = 1u << 20});
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx();
  auto rx = channel.rx();

  std::vector<std::byte> payload(128 * 1024);
  for (std::size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<std::byte>(i & 0xFFu);
  }

  auto offered = tx.offer(payload);
  ASSERT_TRUE(offered);

  auto message = rx.try_recv();
  ASSERT_TRUE(message);
  ASSERT_EQ(message->payload.size(), payload.size());
  EXPECT_TRUE(std::equal(message->payload.begin(), message->payload.end(), payload.begin()));
  rx.release(*message);
}

// 验证 bulk 模式仍会拒绝无法放入单帧的 payload。
TEST(BulkChannelTest, RejectsMessageThatCannotFitInOneFrame) {
  auto created = BulkChannel<>::create(ChannelConfig{.capacity = 4096});
  ASSERT_TRUE(created);

  auto channel = std::move(created).value();
  auto tx = channel.tx();

  std::vector<std::byte> payload(4096, std::byte{0x7F});
  auto offered = tx.offer(payload);
  ASSERT_FALSE(offered);
  EXPECT_EQ(offered.error(), FlowError::MessageTooLarge);
}

}  // namespace
