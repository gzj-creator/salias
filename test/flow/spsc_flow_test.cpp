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

namespace {

struct FlowFixture {
  salias::platform::Mapping mapping;
  salias::ring::MagicRing ring;
  std::uint64_t producer_pos = 0;
  std::uint64_t consumer_pos = 0;

  salias::flow::Positions positions() noexcept {
    return salias::flow::Positions{
        .producer = &producer_pos,
        .consumer = &consumer_pos,
        .cap = ring.capacity(),
    };
  }
};

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

  EXPECT_EQ(fixture.producer_pos, salias::frame::frame_len(payload.size()));
  auto message = consumer.poll();
  ASSERT_TRUE(message.has_value());
  ASSERT_EQ(message->payload.size(), payload.size());
  for (std::size_t i = 0; i < payload.size(); ++i) {
    EXPECT_EQ(message->payload[i], payload[i]) << "payload byte " << i;
  }

  consumer.advance(message->next_position);
  EXPECT_EQ(fixture.consumer_pos, fixture.producer_pos);
  EXPECT_FALSE(consumer.poll().has_value());
}

TEST(SpscFlowTest, BackPressureClearsImmediatelyAfterAdvance) {
  FlowFixture fixture = make_fixture();
  salias::flow::Producer producer(fixture.ring, fixture.positions());
  salias::flow::Consumer consumer(fixture.ring, fixture.positions());

  const std::uint32_t payload_len =
      static_cast<std::uint32_t>(fixture.ring.capacity() - salias::frame::kHeaderSize);
  auto claim = producer.claim(payload_len);
  ASSERT_TRUE(claim);
  producer.commit(claim.value());

  auto blocked = producer.claim(1);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::flow::FlowError::BackPressured);

  auto message = consumer.poll();
  ASSERT_TRUE(message);
  consumer.advance(message->next_position);

  auto unblocked = producer.claim(1);
  ASSERT_TRUE(unblocked);
}

TEST(SpscFlowTest, RejectsPayloadLargerThanSingleRingCapacity) {
  FlowFixture fixture = make_fixture();
  salias::flow::Producer producer(fixture.ring, fixture.positions());

  auto claim = producer.claim(static_cast<std::uint32_t>(fixture.ring.capacity()));

  ASSERT_FALSE(claim);
  EXPECT_EQ(claim.error(), salias::flow::FlowError::MessageTooLarge);
}

}  // namespace
