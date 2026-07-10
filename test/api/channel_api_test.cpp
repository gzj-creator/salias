#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "salias/salias.hpp"

namespace {

static_assert(!std::is_copy_constructible_v<salias::PublishClaim<salias::Mode::FifoMpsc>>);
static_assert(!std::is_same_v<salias::FifoMpscChannel, salias::OrderedMpscChannel>);
static_assert(!std::is_same_v<salias::FifoFanoutChannel, salias::OrderedFanoutChannel>);

struct Payload {
  std::uint32_t producer = 0;
  std::uint32_t sequence = 0;
};

std::string unique_name(std::string_view suffix) {
  static std::atomic<std::uint32_t> counter = 0;
  return "channel-api-" + std::to_string(::getpid()) + "-" +
         std::to_string(counter.fetch_add(1, std::memory_order_relaxed)) + "-" +
         std::string(suffix);
}

salias::Config config_for(salias::Mode mode, std::string_view suffix) {
  return salias::Config{
      .name = unique_name(suffix),
      .mode = mode,
      .capacity = 4096,
      .num_producers = 1,
      .num_consumers = 1,
  };
}

std::array<std::byte, sizeof(Payload)> encode(Payload payload) {
  std::array<std::byte, sizeof(Payload)> bytes{};
  std::memcpy(bytes.data(), &payload, sizeof(payload));
  return bytes;
}

Payload decode(std::span<const std::byte> payload) {
  Payload value{};
  std::memcpy(&value, payload.data(), sizeof(value));
  return value;
}

template <salias::Mode M>
salias::Message wait_for_message(salias::Subscriber<M>& subscriber) {
  for (int attempt = 0; attempt < 100000; ++attempt) {
    if (auto message = subscriber.try_recv(); message.has_value()) {
      return *message;
    }
    std::this_thread::yield();
  }
  return {};
}

TEST(ChannelApiTest, FifoMpscClaimOfferAndPersistentSubscriberRoundTrip) {
  auto created =
      salias::FifoMpscChannel::create(config_for(salias::Mode::FifoMpsc, "fifo-roundtrip"));
  ASSERT_TRUE(created);
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto first_claim = publisher.try_claim(sizeof(Payload));
  ASSERT_TRUE(first_claim);
  const Payload first{.producer = 0, .sequence = 1};
  std::memcpy(first_claim->payload().data(), &first, sizeof(first));
  EXPECT_FALSE(subscriber.try_recv().has_value());
  first_claim->commit();

  auto first_message = subscriber.try_recv();
  ASSERT_TRUE(first_message);
  EXPECT_EQ(decode(first_message->payload).sequence, 1u);
  subscriber.release(*first_message);

  const auto second = encode(Payload{.producer = 0, .sequence = 2});
  ASSERT_TRUE(publisher.offer(second));
  auto second_message = subscriber.try_recv();
  ASSERT_TRUE(second_message);
  EXPECT_EQ(second_message->sequence, 1u);
  EXPECT_EQ(decode(second_message->payload).sequence, 2u);
  subscriber.release(*second_message);
}

TEST(ChannelApiTest, PollKeepsReceiverCursorAcrossBatches) {
  auto created =
      salias::FifoMpscChannel::create(config_for(salias::Mode::FifoMpsc, "persistent-poll"));
  ASSERT_TRUE(created);
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 10})));
  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 11})));

  std::vector<std::uint32_t> seen;
  auto handler = [&seen](const salias::Message& message) noexcept {
    seen.push_back(decode(message.payload).sequence);
  };
  EXPECT_EQ(subscriber.poll(1, handler), 1u);
  EXPECT_EQ(subscriber.poll(1, handler), 1u);
  EXPECT_EQ(seen, (std::vector<std::uint32_t>{10, 11}));
}

TEST(ChannelApiTest, FifoMpscConsumesReadyProducerWithoutGlobalGap) {
  auto config = config_for(salias::Mode::FifoMpsc, "fifo-order");
  config.num_producers = 2;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(created).value();
  auto first_publisher = channel.publisher();
  auto second_publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto first = first_publisher.try_claim(sizeof(Payload));
  ASSERT_TRUE(first);
  ASSERT_TRUE(second_publisher.offer(encode(Payload{.producer = 1, .sequence = 20})));

  auto second_message = subscriber.try_recv();
  ASSERT_TRUE(second_message);
  EXPECT_EQ(second_message->producer_id, 1u);
  subscriber.release(*second_message);

  const Payload first_payload{.producer = 0, .sequence = 10};
  std::memcpy(first->payload().data(), &first_payload, sizeof(first_payload));
  first->commit();
  auto first_message = subscriber.try_recv();
  ASSERT_TRUE(first_message);
  EXPECT_EQ(first_message->producer_id, 0u);
  subscriber.release(*first_message);
}

TEST(ChannelApiTest, OrderedMpscWaitsForEarlierGlobalSequence) {
  auto config = config_for(salias::Mode::OrderedMpsc, "ordered-mpsc");
  config.num_producers = 2;
  auto created = salias::OrderedMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(created).value();
  auto first_publisher = channel.publisher();
  auto second_publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto first = first_publisher.try_claim(sizeof(Payload));
  auto second = second_publisher.try_claim(sizeof(Payload));
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  const Payload second_payload{.producer = 1, .sequence = 20};
  std::memcpy(second->payload().data(), &second_payload, sizeof(second_payload));
  second->commit();
  EXPECT_FALSE(subscriber.try_recv().has_value());

  const Payload first_payload{.producer = 0, .sequence = 10};
  std::memcpy(first->payload().data(), &first_payload, sizeof(first_payload));
  first->commit();
  auto first_message = subscriber.try_recv();
  ASSERT_TRUE(first_message);
  EXPECT_EQ(first_message->sequence, 0u);
  subscriber.release(*first_message);
  auto second_message = subscriber.try_recv();
  ASSERT_TRUE(second_message);
  EXPECT_EQ(second_message->sequence, 1u);
  subscriber.release(*second_message);
}

TEST(ChannelApiTest, FifoFanoutDeliversEachMessageToEverySubscriber) {
  auto config = config_for(salias::Mode::FifoFanout, "fifo-fanout");
  config.num_consumers = 2;
  auto created = salias::FifoFanoutChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto first_subscriber = channel.subscriber();
  auto second_subscriber = channel.subscriber();

  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 7})));
  auto first = first_subscriber.try_recv();
  auto second = second_subscriber.try_recv();
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  EXPECT_EQ(decode(first->payload).sequence, 7u);
  EXPECT_EQ(decode(second->payload).sequence, 7u);
  first_subscriber.release(*first);
  second_subscriber.release(*second);
}

TEST(ChannelApiTest, OrderedFanoutPreservesGlobalOrderForEverySubscriber) {
  auto config = config_for(salias::Mode::OrderedFanout, "ordered-fanout");
  config.num_producers = 2;
  config.num_consumers = 2;
  auto created = salias::OrderedFanoutChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(created).value();
  auto first_publisher = channel.publisher();
  auto second_publisher = channel.publisher();
  auto first_subscriber = channel.subscriber();
  auto second_subscriber = channel.subscriber();

  auto first = first_publisher.try_claim(sizeof(Payload));
  auto second = second_publisher.try_claim(sizeof(Payload));
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  second->commit();
  EXPECT_FALSE(first_subscriber.try_recv().has_value());
  EXPECT_FALSE(second_subscriber.try_recv().has_value());
  first->commit();

  auto first_a = first_subscriber.try_recv();
  auto first_b = second_subscriber.try_recv();
  ASSERT_TRUE(first_a);
  ASSERT_TRUE(first_b);
  EXPECT_EQ(first_a->sequence, 0u);
  EXPECT_EQ(first_b->sequence, 0u);
  first_subscriber.release(*first_a);
  second_subscriber.release(*first_b);

  auto second_a = first_subscriber.try_recv();
  auto second_b = second_subscriber.try_recv();
  ASSERT_TRUE(second_a);
  ASSERT_TRUE(second_b);
  EXPECT_EQ(second_a->sequence, 1u);
  EXPECT_EQ(second_b->sequence, 1u);
  first_subscriber.release(*second_a);
  second_subscriber.release(*second_b);
}

TEST(ChannelApiTest, OfferBatchPublishesContiguousMessages) {
  auto created = salias::FifoMpscChannel::create(config_for(salias::Mode::FifoMpsc, "batch"));
  ASSERT_TRUE(created);
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  const auto first = encode(Payload{.sequence = 1});
  const auto second = encode(Payload{.sequence = 2});
  const std::array<std::span<const std::byte>, 2> payloads{first, second};
  auto published = publisher.offer_batch(payloads);
  ASSERT_TRUE(published);
  EXPECT_EQ(published.value(), 2u);

  for (std::uint32_t expected = 1; expected <= 2; ++expected) {
    auto message = subscriber.try_recv();
    ASSERT_TRUE(message);
    EXPECT_EQ(decode(message->payload).sequence, expected);
    subscriber.release(*message);
  }
}

TEST(ChannelApiTest, ConnectRejectsDifferentMode) {
  auto config = config_for(salias::Mode::FifoMpsc, "mode-mismatch");
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto connected = salias::OrderedMpscChannel::connect(config.name);
  ASSERT_FALSE(connected);
  EXPECT_EQ(connected.error(), salias::Error::BadConfig);
}

TEST(ChannelApiTest, RejectsInvalidProducerAndConsumerCounts) {
  auto producer_config = config_for(salias::Mode::FifoMpsc, "bad-producers");
  producer_config.num_producers = 0;
  auto no_producers = salias::FifoMpscChannel::create(producer_config);
  ASSERT_FALSE(no_producers);
  EXPECT_EQ(no_producers.error(), salias::Error::BadConfig);

  auto single_config = config_for(salias::Mode::OrderedMpsc, "bad-single-consumers");
  single_config.num_consumers = 2;
  auto multiple_consumers = salias::OrderedMpscChannel::create(single_config);
  ASSERT_FALSE(multiple_consumers);
  EXPECT_EQ(multiple_consumers.error(), salias::Error::BadConfig);

  auto fanout_config = config_for(salias::Mode::FifoFanout, "bad-fanout-consumers");
  fanout_config.num_consumers = 9;
  auto too_many_consumers = salias::FifoFanoutChannel::create(fanout_config);
  ASSERT_FALSE(too_many_consumers);
  EXPECT_EQ(too_many_consumers.error(), salias::Error::BadConfig);
}

TEST(ChannelApiTest, ExhaustedEndpointSlotsReturnBadConfigOnUse) {
  auto config = config_for(salias::Mode::FifoFanout, "slot-exhaustion");
  config.num_producers = 1;
  config.num_consumers = 1;
  auto created = salias::FifoFanoutChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(created).value();
  auto first_publisher = channel.publisher();
  auto exhausted_publisher = channel.publisher();
  auto first_subscriber = channel.subscriber();
  auto exhausted_subscriber = channel.subscriber();

  ASSERT_TRUE(first_publisher.offer(encode(Payload{.sequence = 1})));
  auto rejected = exhausted_publisher.offer(encode(Payload{.sequence = 2}));
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), salias::Error::BadConfig);
  EXPECT_FALSE(exhausted_subscriber.try_recv().has_value());

  auto message = first_subscriber.try_recv();
  ASSERT_TRUE(message);
  first_subscriber.release(*message);
}

TEST(ChannelApiTest, HugePageCapacityMustBeAligned) {
  auto config = config_for(salias::Mode::FifoMpsc, "huge-alignment");
  config.huge = salias::HugePage::Size2MB;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_FALSE(created);
  EXPECT_EQ(created.error(), salias::Error::BadConfig);
}

TEST(ChannelApiTest, MultiplePublisherProcessesShareProducerSlots) {
  auto config = config_for(salias::Mode::FifoMpsc, "fork-publishers");
  config.num_producers = 2;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto owner = std::move(created).value();
  auto subscriber = owner.subscriber();

  std::array<pid_t, 2> children{};
  for (std::uint32_t producer = 0; producer < children.size(); ++producer) {
    children[producer] = ::fork();
    ASSERT_GE(children[producer], 0);
    if (children[producer] == 0) {
      auto connected = salias::FifoMpscChannel::connect(config.name);
      if (!connected) {
        ::_exit(10);
      }
      auto peer = std::move(connected).value();
      auto publisher = peer.publisher();
      auto offered = publisher.offer(encode(Payload{.producer = producer, .sequence = 1}));
      ::_exit(offered ? 0 : 11);
    }
  }

  std::array<bool, 2> seen{};
  for (std::size_t received = 0; received < children.size(); ++received) {
    auto message = wait_for_message(subscriber);
    ASSERT_FALSE(message.payload.empty());
    const Payload payload = decode(message.payload);
    ASSERT_LT(payload.producer, seen.size());
    seen[payload.producer] = true;
    subscriber.release(message);
  }
  EXPECT_TRUE(seen[0]);
  EXPECT_TRUE(seen[1]);

  for (const pid_t child : children) {
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
  }
}

}  // namespace
