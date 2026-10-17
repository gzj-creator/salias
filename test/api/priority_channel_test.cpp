/**
 * @file test/api/priority_channel_test.cpp
 * @brief Fair scheduling and independent backpressure tests for priority endpoints.
 */
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
#include <string_view>
#include <thread>
#include <utility>

#include "salias/salias.hpp"

namespace {

using Mode = salias::Mode;
using Fifo = salias::Channel<Mode::FifoMpsc>;
using PriorityPub = salias::PriorityPublisher<Mode::FifoMpsc, Mode::FifoMpsc>;
using PrioritySub = salias::PrioritySubscriber<Mode::FifoMpsc, Mode::FifoMpsc>;
using ThreePriorityPub = salias::PriorityPublisher<Mode::FifoMpsc, Mode::FifoMpsc, Mode::FifoMpsc>;
using ThreePrioritySub = salias::PrioritySubscriber<Mode::FifoMpsc, Mode::FifoMpsc, Mode::FifoMpsc>;

std::string unique_name(std::string_view suffix) {
  static std::atomic<std::uint64_t> next{0};
  return "priority-" + std::to_string(::getpid()) + "-" + std::string(suffix) + "-" +
         std::to_string(next.fetch_add(1, std::memory_order_relaxed));
}

salias::Config config(std::string name, std::size_t publication_window = 0) {
  salias::Config result;
  result.name = std::move(name);
  result.capacity = 4096;
  result.publication_window = publication_window;
  return result;
}

std::span<const std::byte> bytes_of(const std::uint64_t& value) {
  return std::as_bytes(std::span{&value, 1});
}

std::uint64_t read_value(const salias::PriorityMessage& message) {
  EXPECT_EQ(message.message.payload.size(), sizeof(std::uint64_t));
  std::uint64_t value = 0;
  if (message.message.payload.size() == sizeof(value)) {
    std::memcpy(&value, message.message.payload.data(), sizeof(value));
  }
  return value;
}

TEST(PriorityChannelTest, WeightedRoundRobinPreservesPerStreamOrderWithoutStarvation) {
  auto high_created = Fifo::create(config(unique_name("high")));
  auto low_created = Fifo::create(config(unique_name("low")));
  ASSERT_TRUE(high_created);
  ASSERT_TRUE(low_created);

  auto high_subscriber = high_created->subscriber();
  auto low_subscriber = low_created->subscriber();
  PrioritySub reader(std::move(high_subscriber), std::move(low_subscriber));
  PriorityPub publisher(high_created->publisher(), low_created->publisher());

  for (std::uint64_t value = 0; value < 8; ++value) {
    ASSERT_TRUE(publisher.offer<0>(bytes_of(value)));
    if (value < 4) {
      ASSERT_TRUE(publisher.offer(1, bytes_of(value + 100)));
    }
  }

  constexpr std::array<std::uint32_t, 12> expected_priorities = {0, 0, 1, 0, 0, 1,
                                                                 0, 0, 1, 0, 0, 1};
  std::array<std::uint64_t, 2> next_value{};
  for (const std::uint32_t expected_priority : expected_priorities) {
    auto received = reader.try_recv_checked();
    ASSERT_TRUE(received);
    ASSERT_TRUE(*received);
    EXPECT_EQ((**received).priority, expected_priority);
    const std::uint64_t value = read_value(**received);
    EXPECT_EQ(value, next_value[expected_priority] + (expected_priority == 0 ? 0 : 100));
    ++next_value[expected_priority];
    ASSERT_TRUE(reader.release(**received));
  }
  EXPECT_EQ(next_value[0], 8u);
  EXPECT_EQ(next_value[1], 4u);

  auto empty = reader.try_recv_checked();
  ASSERT_TRUE(empty);
  EXPECT_FALSE(*empty);
}

TEST(PriorityChannelTest, ThreeLevelsReceiveThreeToTwoToOneService) {
  auto high = Fifo::create(config(unique_name("three-high")));
  auto medium = Fifo::create(config(unique_name("three-medium")));
  auto low = Fifo::create(config(unique_name("three-low")));
  ASSERT_TRUE(high);
  ASSERT_TRUE(medium);
  ASSERT_TRUE(low);
  ThreePrioritySub reader(high->subscriber(), medium->subscriber(), low->subscriber());
  ThreePriorityPub publisher(high->publisher(), medium->publisher(), low->publisher());

  const std::array<std::uint32_t, 3> counts{3, 2, 1};
  for (std::size_t priority = 0; priority < counts.size(); ++priority) {
    for (std::uint32_t sequence = 0; sequence < counts[priority]; ++sequence) {
      const std::uint64_t value = priority * 100 + sequence;
      ASSERT_TRUE(publisher.offer(priority, bytes_of(value)));
    }
  }

  constexpr std::array<std::uint32_t, 6> expected{0, 0, 0, 1, 1, 2};
  std::array<std::uint32_t, 3> next_sequence{};
  for (const std::uint32_t priority : expected) {
    auto received = reader.try_recv_checked();
    ASSERT_TRUE(received);
    ASSERT_TRUE(*received);
    EXPECT_EQ((**received).priority, priority);
    EXPECT_EQ(read_value(**received), priority * 100 + next_sequence[priority]++);
    ASSERT_TRUE(reader.release(**received));
  }
  EXPECT_EQ(next_sequence, counts);
}

TEST(PriorityChannelTest, BackpressureAndInvalidPriorityStayLocalToTheirStream) {
  auto high_created = Fifo::create(config(unique_name("bounded-high"), 4096));
  auto low_created = Fifo::create(config(unique_name("bounded-low"), 4096));
  ASSERT_TRUE(high_created);
  ASSERT_TRUE(low_created);

  auto reader = PrioritySub(high_created->subscriber(), low_created->subscriber());
  PriorityPub publisher(high_created->publisher(), low_created->publisher());
  const std::array<std::byte, 256> payload{};
  std::size_t accepted = 0;
  for (;;) {
    auto offered = publisher.offer(0, payload);
    if (offered) {
      ++accepted;
      continue;
    }
    ASSERT_EQ(offered.error(), salias::Error::BackPressured);
    break;
  }
  EXPECT_GT(accepted, 0u);

  auto low_offer = publisher.offer(1, payload);
  ASSERT_TRUE(low_offer);
  EXPECT_TRUE(*low_offer);
  auto invalid_priority = publisher.offer(2, payload);
  ASSERT_FALSE(invalid_priority);
  EXPECT_EQ(invalid_priority.error(), salias::Error::BadConfig);

  auto next = reader.try_recv_checked();
  ASSERT_TRUE(next);
  ASSERT_TRUE(*next);
  EXPECT_EQ((**next).priority, 0u);
  ASSERT_TRUE(reader.release(**next));
  EXPECT_TRUE(publisher.offer(0, payload));
}

TEST(PriorityChannelTest, DefaultAndMovedFromEndpointsReturnBadConfig) {
  PriorityPub invalid_publisher;
  const std::uint64_t value = 7;
  auto bad_offer = invalid_publisher.offer(0, bytes_of(value));
  ASSERT_FALSE(bad_offer);
  EXPECT_EQ(bad_offer.error(), salias::Error::BadConfig);
  auto invalid_underlying = invalid_publisher.publisher<0>();
  ASSERT_FALSE(invalid_underlying);
  EXPECT_EQ(invalid_underlying.error(), salias::Error::BadConfig);

  PrioritySub invalid_subscriber;
  auto bad_receive = invalid_subscriber.try_recv_checked();
  ASSERT_FALSE(bad_receive);
  EXPECT_EQ(bad_receive.error(), salias::Error::BadConfig);

  auto high_created = Fifo::create(config(unique_name("move-high")));
  auto low_created = Fifo::create(config(unique_name("move-low")));
  ASSERT_TRUE(high_created);
  ASSERT_TRUE(low_created);
  PrioritySub reader(high_created->subscriber(), low_created->subscriber());
  PriorityPub publisher(high_created->publisher(), low_created->publisher());
  PriorityPub moved_publisher(std::move(publisher));
  EXPECT_FALSE(publisher);
  auto moved_from_offer = publisher.offer(0, bytes_of(value));
  ASSERT_FALSE(moved_from_offer);
  EXPECT_EQ(moved_from_offer.error(), salias::Error::BadConfig);
  EXPECT_TRUE(moved_publisher.offer(0, bytes_of(value)));

  PrioritySub moved_reader(std::move(reader));
  EXPECT_FALSE(reader);
  auto moved_from_receive = reader.try_recv_checked();
  ASSERT_FALSE(moved_from_receive);
  EXPECT_EQ(moved_from_receive.error(), salias::Error::BadConfig);
  auto message = moved_reader.try_recv_checked();
  ASSERT_TRUE(message);
  ASSERT_TRUE(*message);
  EXPECT_EQ((**message).priority, 0u);
  ASSERT_TRUE(moved_reader.release(**message));

  std::atomic<bool> publish_ok{false};
  std::thread delayed_publisher([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    const auto offered = moved_publisher.offer(1, bytes_of(value));
    publish_ok.store(offered && *offered, std::memory_order_release);
  });
  auto blocking_message = moved_reader.recv();
  delayed_publisher.join();
  ASSERT_TRUE(blocking_message);
  EXPECT_EQ(blocking_message->priority, 1u);
  EXPECT_EQ(read_value(*blocking_message), value);
  EXPECT_TRUE(publish_ok.load(std::memory_order_acquire));
  ASSERT_TRUE(moved_reader.release(*blocking_message));
}

TEST(PriorityChannelTest, OutstandingBorrowIsNotReturnedTwiceAndPollReleasesIt) {
  auto high_created = Fifo::create(config(unique_name("borrow-high")));
  auto low_created = Fifo::create(config(unique_name("borrow-low")));
  ASSERT_TRUE(high_created);
  ASSERT_TRUE(low_created);
  PrioritySub reader(high_created->subscriber(), low_created->subscriber());
  PriorityPub publisher(high_created->publisher(), low_created->publisher());

  const std::uint64_t high_value = 1;
  const std::uint64_t low_value = 2;
  ASSERT_TRUE(publisher.offer(0, bytes_of(high_value)));
  ASSERT_TRUE(publisher.offer(1, bytes_of(low_value)));

  auto high = reader.try_recv_checked();
  ASSERT_TRUE(high);
  ASSERT_TRUE(*high);
  EXPECT_EQ((**high).priority, 0u);
  auto low = reader.try_recv_checked();
  ASSERT_TRUE(low);
  ASSERT_TRUE(*low);
  EXPECT_EQ((**low).priority, 1u);
  auto no_duplicate = reader.try_recv_checked();
  ASSERT_TRUE(no_duplicate);
  EXPECT_FALSE(*no_duplicate);

  ASSERT_TRUE(reader.release(**high));
  ASSERT_TRUE(reader.release(**low));
  auto duplicate_release = reader.release(**low);
  ASSERT_FALSE(duplicate_release);
  EXPECT_EQ(duplicate_release.error(), salias::Error::BadConfig);

  ASSERT_TRUE(publisher.offer(0, bytes_of(high_value)));
  ASSERT_TRUE(publisher.offer(1, bytes_of(low_value)));
  std::array<std::uint32_t, 2> seen{};
  const auto consumed = reader.poll(
      2, [&seen](const salias::PriorityMessage& message) noexcept { ++seen[message.priority]; });
  EXPECT_EQ(consumed, 2u);
  EXPECT_EQ(seen[0], 1u);
  EXPECT_EQ(seen[1], 1u);
  auto empty = reader.try_recv_checked();
  ASSERT_TRUE(empty);
  EXPECT_FALSE(*empty);
}

TEST(PriorityChannelTest, IndependentProcessPublishesToSeparatePriorityStreams) {
  auto high_config = config(unique_name("ipc-high"));
  auto low_config = config(unique_name("ipc-low"));
  high_config.num_producers = 2;
  low_config.num_producers = 2;
  auto high_created = Fifo::create(high_config);
  auto low_created = Fifo::create(low_config);
  ASSERT_TRUE(high_created);
  ASSERT_TRUE(low_created);
  PrioritySub reader(high_created->subscriber(), low_created->subscriber());

  int result_pipe[2]{};
  ASSERT_EQ(::pipe(result_pipe), 0);
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(result_pipe[0]));
    auto high = Fifo::connect(high_config.name);
    auto low = Fifo::connect(low_config.name);
    if (!high || !low) {
      const char failed = 'e';
      const auto written = ::write(result_pipe[1], &failed, 1);
      static_cast<void>(written);
      ::_exit(2);
    }
    PriorityPub publisher(high->publisher(), low->publisher());
    const std::uint64_t high_value = 10;
    const std::uint64_t low_value = 20;
    const auto high_offered = publisher.offer(0, bytes_of(high_value));
    const auto low_offered = publisher.offer(1, bytes_of(low_value));
    const char result = high_offered && low_offered ? 'o' : 'e';
    const auto written = ::write(result_pipe[1], &result, 1);
    static_cast<void>(written);
    ::_exit(result == 'o' ? 0 : 3);
  }

  static_cast<void>(::close(result_pipe[1]));
  char result = 0;
  ASSERT_EQ(::read(result_pipe[0], &result, 1), 1);
  static_cast<void>(::close(result_pipe[0]));
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);
  ASSERT_EQ(result, 'o');

  auto high = reader.try_recv_checked();
  ASSERT_TRUE(high);
  ASSERT_TRUE(*high);
  EXPECT_EQ((**high).priority, 0u);
  EXPECT_EQ(read_value(**high), 10u);
  ASSERT_TRUE(reader.release(**high));
  auto low = reader.try_recv_checked();
  ASSERT_TRUE(low);
  ASSERT_TRUE(*low);
  EXPECT_EQ((**low).priority, 1u);
  EXPECT_EQ(read_value(**low), 20u);
  ASSERT_TRUE(reader.release(**low));
}

TEST(PriorityChannelTest, LaggedHighPriorityStreamDoesNotBlockLowerPriorityData) {
  using Latest = salias::LatestFanoutChannel<4096>;
  using MixedPublisher = salias::PriorityPublisher<Mode::LatestFanout, Mode::FifoMpsc>;
  using MixedReader = salias::PrioritySubscriber<Mode::LatestFanout, Mode::FifoMpsc>;
  auto high_config = config(unique_name("lagged-high"));
  high_config.mode = Mode::LatestFanout;
  auto high_created = Latest::create(high_config);
  auto low_created = Fifo::create(config(unique_name("lagged-low")));
  ASSERT_TRUE(high_created);
  ASSERT_TRUE(low_created);
  MixedReader reader(high_created->subscriber(), low_created->subscriber());
  MixedPublisher publisher(high_created->publisher(), low_created->publisher());

  const std::uint64_t high_value = 55;
  for (std::uint32_t index = 0; index < 400; ++index) {
    ASSERT_TRUE(publisher.offer(0, bytes_of(high_value)));
  }
  const std::uint64_t low_value = 77;
  ASSERT_TRUE(publisher.offer(1, bytes_of(low_value)));

  auto received = reader.try_recv_checked();
  ASSERT_TRUE(received);
  ASSERT_TRUE(*received);
  EXPECT_EQ((**received).priority, 1u);
  EXPECT_EQ(read_value(**received), low_value);
  EXPECT_GT(reader.subscriber<0>().lagged_messages(), 0u);
  ASSERT_TRUE(reader.release(**received));
}

}  // namespace
