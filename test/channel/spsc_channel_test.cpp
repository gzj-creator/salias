#include "core/channel/spsc.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstring>
#include <vector>

namespace {

// 为测试创建页大小的 SPSC 通道配置。
salias::channel::ChannelConfig test_config() {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  EXPECT_GT(raw_page_size, 0);
  return salias::channel::ChannelConfig{.capacity = static_cast<std::size_t>(raw_page_size)};
}

// 验证 SPSC offer、receive 和 release 能完成 payload 往返。
TEST(SpscChannelTest, OfferTryRecvAndReleaseRoundTripsPayload) {
  auto channel_result = salias::channel::SpscChannel<>::create(test_config());
  ASSERT_TRUE(channel_result);

  auto channel = std::move(channel_result).value();
  auto tx = channel.tx();
  auto rx = channel.rx();

  constexpr std::array payload{
      std::byte{0x31}, std::byte{0x32}, std::byte{0x33}, std::byte{0x34},
      std::byte{0x35}, std::byte{0x36}, std::byte{0x37}, std::byte{0x38},
  };

  auto offered = tx.offer(payload);
  ASSERT_TRUE(offered);

  auto message = rx.try_recv();
  ASSERT_TRUE(message);
  ASSERT_EQ(message->payload.size(), payload.size());
  for (std::size_t i = 0; i < payload.size(); ++i) {
    EXPECT_EQ(message->payload[i], payload[i]) << "payload byte " << i;
  }

  rx.release(*message);
  EXPECT_FALSE(rx.try_recv().has_value());
}

// 验证超过 ring 容量的 offer 会被拒绝。
TEST(SpscChannelTest, RejectsTooLargeOffer) {
  auto channel_result = salias::channel::SpscChannel<>::create(test_config());
  ASSERT_TRUE(channel_result);

  auto channel = std::move(channel_result).value();
  auto tx = channel.tx();
  std::vector<std::byte> payload(channel.capacity());

  auto offered = tx.offer(payload);

  ASSERT_FALSE(offered);
  EXPECT_EQ(offered.error(), salias::flow::FlowError::MessageTooLarge);
}

}  // namespace
