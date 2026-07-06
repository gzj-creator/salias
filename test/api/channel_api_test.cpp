#include "salias/salias.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cstddef>

namespace {

salias::Config test_config() {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  EXPECT_GT(raw_page_size, 0);
  salias::Config config;
  config.capacity = static_cast<std::size_t>(raw_page_size);
  return config;
}

TEST(ChannelApiTest, InProcessSpscOfferAndTryRecv) {
  auto channel_result = salias::Channel::create(test_config());
  ASSERT_TRUE(channel_result);
  auto channel = std::move(channel_result).value();

  salias::Publisher publisher = channel.publisher();
  salias::Subscriber subscriber = channel.subscriber();

  constexpr std::array payload{
      std::byte{0x61},
      std::byte{0x62},
      std::byte{0x63},
  };

  auto offered = publisher.offer(payload);
  ASSERT_TRUE(offered);
  EXPECT_TRUE(offered.value());

  auto message = subscriber.try_recv();
  ASSERT_TRUE(message.has_value());
  ASSERT_EQ(message->payload.size(), payload.size());
  for (std::size_t i = 0; i < payload.size(); ++i) {
    EXPECT_EQ(message->payload[i], payload[i]) << "payload byte " << i;
  }

  subscriber.release(*message);
  EXPECT_FALSE(subscriber.try_recv().has_value());
}

TEST(ChannelApiTest, RejectsUnsupportedNamedHandshakeForNow) {
  salias::Config config = test_config();
  config.name = "named-channel";

  auto channel = salias::Channel::create(config);

  ASSERT_FALSE(channel);
  EXPECT_EQ(channel.error(), salias::Error::BadConfig);
}

}  // namespace
