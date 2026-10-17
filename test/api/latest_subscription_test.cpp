/**
 * @file test/api/latest_subscription_test.cpp
 * @brief Explicit latest seek, eviction reporting and loss accounting over named IPC.
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
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>

#include "salias/salias.hpp"

namespace {

std::string channel_name() {
  static std::atomic<std::uint32_t> next{0};
  return "salias-seek-" + std::to_string(::getpid()) + "-" +
         std::to_string(next.fetch_add(1, std::memory_order_relaxed));
}

std::uint64_t decode(const salias::Message& message) {
  std::uint64_t value = 0;
  std::memcpy(&value, message.payload.data(), sizeof(value));
  return value;
}

class LatestSubscriptionTest : public ::testing::Test {
 protected:
  using Channel = salias::LatestMpmcChannel<4096, 50>;

  void SetUp() override {
    config_.name = channel_name();
    config_.num_producers = 2;
    config_.num_consumers = 3;
    auto created = Channel::create(config_);
    ASSERT_TRUE(created);
    channel_.emplace(std::move(*created));
  }

  static bool offer(salias::LatestMpmcPublisher& publisher, std::uint64_t sequence) {
    return publisher.offer(std::as_bytes(std::span{&sequence, 1})).has_value();
  }

  salias::Config config_;
  std::optional<Channel> channel_;
};

TEST_F(LatestSubscriptionTest, CheckedReceiveReportsEachObservedGapOnceBeforeDelivery) {
  auto publisher = channel_->publisher();
  auto subscriber = channel_->subscriber();
  auto empty = subscriber.try_recv_checked();
  ASSERT_TRUE(empty);
  EXPECT_FALSE(*empty);
  for (std::uint64_t sequence = 0; sequence < 300; ++sequence) {
    ASSERT_TRUE(offer(publisher, sequence));
  }
  auto lagged = subscriber.try_recv_checked();
  ASSERT_FALSE(lagged);
  EXPECT_EQ(lagged.error(), salias::Error::Lagged);
  EXPECT_EQ(subscriber.lagged_messages(), 172u);
  EXPECT_EQ(subscriber.skipped_messages(), 0u);
  auto message = subscriber.try_recv_checked();
  ASSERT_TRUE(message);
  ASSERT_TRUE(*message);
  EXPECT_EQ((*message)->sequence, 172u);
  EXPECT_EQ(decode(**message), 172u);
  auto pinned = subscriber.try_recv_checked();
  ASSERT_TRUE(pinned);
  EXPECT_FALSE(*pinned);
  subscriber.release(**message);
  for (std::uint64_t sequence = 300; sequence < 600; ++sequence) {
    ASSERT_TRUE(offer(publisher, sequence));
  }
  auto second_gap = subscriber.try_recv_checked();
  ASSERT_FALSE(second_gap);
  EXPECT_EQ(second_gap.error(), salias::Error::Lagged);
  EXPECT_EQ(subscriber.lagged_messages(), 172u + (472u - 173u));
  auto next = subscriber.try_recv_checked();
  ASSERT_TRUE(next);
  ASSERT_TRUE(*next);
  EXPECT_EQ((*next)->sequence, 472u);
  subscriber.release(**next);
}

TEST_F(LatestSubscriptionTest, SingleAndBothPollOverloadsAccountForLossIndependently) {
  auto publisher = channel_->publisher();
  auto single = channel_->subscriber();
  auto callable = channel_->subscriber();
  auto callback = channel_->subscriber();
  for (std::uint64_t sequence = 0; sequence < 300; ++sequence) {
    ASSERT_TRUE(offer(publisher, sequence));
  }
  auto first = single.try_recv();
  ASSERT_TRUE(first);
  EXPECT_EQ(first->sequence, 172u);
  single.release(*first);
  EXPECT_EQ(single.lagged_messages(), 172u);
  EXPECT_EQ(callable.lagged_messages(), 0u);
  std::uint64_t expected = 172;
  EXPECT_EQ(callable.poll(1000, [&](const salias::Message& message) noexcept {
              EXPECT_EQ(decode(message), expected++);
            }),
            128u);
  EXPECT_EQ(expected, 300u);
  EXPECT_EQ(callable.lagged_messages(), 172u);
  struct Context {
    std::uint64_t next = 172;
  } context;
  const auto check = [](const salias::Message& message, void* user) noexcept {
    auto& state = *static_cast<Context*>(user);
    EXPECT_EQ(decode(message), state.next++);
  };
  EXPECT_EQ(callback.poll(1000, check, &context), 128u);
  EXPECT_EQ(context.next, 300u);
  EXPECT_EQ(callback.lagged_messages(), 172u);
  EXPECT_EQ(single.poll(1000, [](const salias::Message&) noexcept {}), 127u);
  EXPECT_EQ(single.lagged_messages(), 172u);
  EXPECT_EQ(single.skipped_messages(), 0u);
}

TEST_F(LatestSubscriptionTest, SeekSeparatesEvictionsFromExplicitSkipsAndKeepsNewestUnread) {
  auto publisher = channel_->publisher();
  auto subscriber = channel_->subscriber();
  auto independent = channel_->subscriber();
  for (std::uint64_t sequence = 0; sequence < 300; ++sequence) {
    ASSERT_TRUE(offer(publisher, sequence));
  }
  auto skipped = subscriber.skip_to_latest();
  ASSERT_TRUE(skipped);
  EXPECT_EQ(*skipped, 299u);
  EXPECT_EQ(subscriber.lagged_messages(), 172u);
  EXPECT_EQ(subscriber.skipped_messages(), 127u);
  auto repeated = subscriber.skip_to_latest();
  ASSERT_TRUE(repeated);
  EXPECT_EQ(*repeated, 0u);
  auto newest = subscriber.try_recv_checked();
  ASSERT_TRUE(newest);
  ASSERT_TRUE(*newest);
  EXPECT_EQ((*newest)->sequence, 299u);
  EXPECT_EQ(decode(**newest), 299u);
  auto pinned = subscriber.skip_to_latest();
  ASSERT_FALSE(pinned);
  EXPECT_EQ(pinned.error(), salias::Error::BackPressured);
  EXPECT_EQ(subscriber.skipped_messages(), 127u);
  subscriber.release(**newest);
  auto empty = subscriber.skip_to_latest();
  ASSERT_TRUE(empty);
  EXPECT_EQ(*empty, 0u);
  auto other = independent.try_recv();
  ASSERT_TRUE(other);
  EXPECT_EQ(other->sequence, 172u);
  independent.release(*other);
  EXPECT_EQ(independent.lagged_messages(), 172u);
  EXPECT_EQ(independent.skipped_messages(), 0u);
}

TEST_F(LatestSubscriptionTest, SeekIsPerProducerAndPreservesOtherBorrowedRings) {
  auto first = channel_->publisher();
  auto second = channel_->publisher();
  auto subscriber = channel_->subscriber();
  for (std::uint64_t sequence = 0; sequence < 4; ++sequence) {
    ASSERT_TRUE(offer(first, sequence));
    ASSERT_TRUE(offer(second, 100 + sequence));
  }
  auto held = subscriber.try_recv();
  ASSERT_TRUE(held);
  ASSERT_EQ(held->producer_id, 0u);
  auto blocked = subscriber.skip_to_latest(0);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::Error::BackPressured);
  auto skipped = subscriber.skip_to_latest(1);
  ASSERT_TRUE(skipped);
  EXPECT_EQ(*skipped, 3u);
  auto newest = subscriber.try_recv_checked();
  ASSERT_TRUE(newest);
  ASSERT_TRUE(*newest);
  EXPECT_EQ((*newest)->producer_id, 1u);
  EXPECT_EQ((*newest)->sequence, 3u);
  EXPECT_EQ(decode(**newest), 103u);
  EXPECT_EQ(decode(*held), 0u);
  subscriber.release(**newest);
  subscriber.release(*held);
  auto next = subscriber.try_recv();
  ASSERT_TRUE(next);
  EXPECT_EQ(next->producer_id, 0u);
  EXPECT_EQ(next->sequence, 1u);
  subscriber.release(*next);
  EXPECT_EQ(subscriber.lagged_messages(), 0u);
}

TEST_F(LatestSubscriptionTest, SeekCountsMixedAndEmptyFramesAcrossManyPhysicalWraps) {
  auto publisher = channel_->publisher();
  auto subscriber = channel_->subscriber();
  constexpr std::array<std::size_t, 5> kLengths{0, 1, 9, 33, 936};
  std::array<std::byte, 936> payload{};
  for (std::uint64_t sequence = 0; sequence < 300; ++sequence) {
    std::memcpy(payload.data(), &sequence, sizeof(sequence));
    const auto length = kLengths[sequence % kLengths.size()];
    ASSERT_TRUE(publisher.offer(std::span<const std::byte>{payload}.first(length)));
  }
  auto skipped = subscriber.skip_to_latest();
  ASSERT_TRUE(skipped);
  EXPECT_EQ(*skipped, 299u);
  EXPECT_GT(subscriber.lagged_messages(), 0u);
  EXPECT_EQ(subscriber.lagged_messages() + subscriber.skipped_messages(), 299u);
  auto latest = subscriber.try_recv_checked();
  ASSERT_TRUE(latest);
  ASSERT_TRUE(*latest);
  EXPECT_EQ((**latest).sequence, 299u);
  EXPECT_EQ((**latest).payload.size(), 936u);
  EXPECT_EQ(decode(**latest), 299u);
  subscriber.release(**latest);
}

TEST_F(LatestSubscriptionTest, SeekDoesNotCrossAnUncommittedPublicationHole) {
  auto publisher = channel_->publisher();
  auto subscriber = channel_->subscriber();
  auto empty = subscriber.skip_to_latest();
  ASSERT_TRUE(empty);
  EXPECT_EQ(*empty, 0u);
  auto first = publisher.try_claim(8);
  auto second = publisher.try_claim(8);
  auto third = publisher.try_claim(8);
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  ASSERT_TRUE(third);
  const std::uint64_t value = 7;
  std::memcpy(first->payload().data(), &value, sizeof(value));
  std::memcpy(second->payload().data(), &value, sizeof(value));
  std::memcpy(third->payload().data(), &value, sizeof(value));
  second->commit();
  third->commit();
  auto gap = subscriber.skip_to_latest();
  ASSERT_TRUE(gap);
  EXPECT_EQ(*gap, 0u);
  EXPECT_FALSE(subscriber.try_recv());
  first->commit();
  auto skipped = subscriber.skip_to_latest();
  ASSERT_TRUE(skipped);
  EXPECT_EQ(*skipped, 2u);
  auto message = subscriber.try_recv_checked();
  ASSERT_TRUE(message);
  ASSERT_TRUE(*message);
  EXPECT_EQ((*message)->sequence, 2u);
  EXPECT_EQ(decode(**message), value);
  subscriber.release(**message);
  EXPECT_EQ(subscriber.lagged_messages(), 0u);
  EXPECT_EQ(subscriber.skipped_messages(), 2u);
}

TEST_F(LatestSubscriptionTest, InvalidMovedAndExhaustedEndpointsReportBadConfig) {
  auto first = channel_->subscriber();
  auto second = channel_->subscriber();
  auto third = channel_->subscriber();
  auto exhausted = channel_->subscriber();
  auto bad_receive = exhausted.try_recv_checked();
  ASSERT_FALSE(bad_receive);
  EXPECT_EQ(bad_receive.error(), salias::Error::BadConfig);
  auto bad_seek = exhausted.skip_to_latest();
  ASSERT_FALSE(bad_seek);
  EXPECT_EQ(bad_seek.error(), salias::Error::BadConfig);
  auto bad_index = first.skip_to_latest(2);
  ASSERT_FALSE(bad_index);
  EXPECT_EQ(bad_index.error(), salias::Error::BadConfig);
  auto moved = std::move(first);
  auto moved_receive = first.try_recv_checked();
  ASSERT_FALSE(moved_receive);
  EXPECT_EQ(moved_receive.error(), salias::Error::BadConfig);
  auto moved_seek = first.skip_to_latest();
  ASSERT_FALSE(moved_seek);
  EXPECT_EQ(moved_seek.error(), salias::Error::BadConfig);
  EXPECT_EQ(first.lagged_messages(), 0u);
  EXPECT_EQ(first.skipped_messages(), 0u);
  EXPECT_TRUE(moved.try_recv_checked());
}

template <salias::Mode M>
void check_reliable_receive() {
  salias::Config config;
  config.name = channel_name();
  config.capacity = 4096;
  auto channel = salias::Channel<M>::create(config);
  ASSERT_TRUE(channel);
  auto publisher = channel->publisher();
  auto subscriber = channel->subscriber();
  auto rejected = subscriber.skip_to_latest();
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), salias::Error::BadConfig);
  const std::uint64_t value = 42;
  ASSERT_TRUE(publisher.offer(std::as_bytes(std::span{&value, 1})));
  auto message = subscriber.try_recv_checked();
  ASSERT_TRUE(message);
  ASSERT_TRUE(*message);
  EXPECT_EQ(decode(**message), value);
  EXPECT_EQ((*message)->sequence, 0u);
  subscriber.release(**message);
  auto empty = subscriber.try_recv_checked();
  ASSERT_TRUE(empty);
  EXPECT_FALSE(*empty);
  EXPECT_EQ(subscriber.lagged_messages(), 0u);
  EXPECT_EQ(subscriber.skipped_messages(), 0u);
}

TEST_F(LatestSubscriptionTest, ReliableModesSupportCheckedReceiveButRejectLossySeek) {
  check_reliable_receive<salias::Mode::FifoMpsc>();
  check_reliable_receive<salias::Mode::FifoMpmc>();
  check_reliable_receive<salias::Mode::OrderedMpsc>();
  check_reliable_receive<salias::Mode::OrderedMpmc>();
}

TEST_F(LatestSubscriptionTest, SubscriberCopiesShareCountersAndSeekProgress) {
  auto publisher = channel_->publisher();
  auto subscriber = channel_->subscriber();
  auto copy = subscriber;
  for (std::uint64_t sequence = 0; sequence < 300; ++sequence) {
    ASSERT_TRUE(offer(publisher, sequence));
  }
  auto lagged = copy.try_recv_checked();
  ASSERT_FALSE(lagged);
  EXPECT_EQ(lagged.error(), salias::Error::Lagged);
  EXPECT_EQ(subscriber.lagged_messages(), 172u);
  auto skipped = subscriber.skip_to_latest();
  ASSERT_TRUE(skipped);
  EXPECT_EQ(*skipped, 127u);
  EXPECT_EQ(copy.skipped_messages(), 127u);
  auto latest = copy.try_recv();
  ASSERT_TRUE(latest);
  EXPECT_EQ(latest->sequence, 299u);
  subscriber.release(*latest);
  EXPECT_FALSE(copy.try_recv());
}

TEST_F(LatestSubscriptionTest, CheckedLagAndSeekWorkAcrossProcesses) {
  auto publisher = channel_->publisher();
  int registered_pipe[2]{};
  int publish_pipe[2]{};
  ASSERT_EQ(::pipe(registered_pipe), 0);
  ASSERT_EQ(::pipe(publish_pipe), 0);
  const auto child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(registered_pipe[0]));
    static_cast<void>(::close(publish_pipe[1]));
    auto connected = Channel::connect(config_.name);
    if (!connected) {
      ::_exit(10);
    }
    auto subscriber = connected->subscriber();
    const char registered = 'r';
    if (::write(registered_pipe[1], &registered, sizeof(registered)) != sizeof(registered)) {
      ::_exit(11);
    }
    char publish = 0;
    if (::read(publish_pipe[0], &publish, sizeof(publish)) != sizeof(publish)) {
      ::_exit(12);
    }
    if (publish != 'p') {
      ::_exit(13);
    }
    auto lagged = subscriber.try_recv_checked();
    if (lagged || lagged.error() != salias::Error::Lagged ||
        subscriber.lagged_messages() != 172) {
      ::_exit(14);
    }
    auto skipped = subscriber.skip_to_latest();
    if (!skipped || *skipped != 127 || subscriber.skipped_messages() != 127) {
      ::_exit(15);
    }
    auto latest = subscriber.try_recv_checked();
    if (!latest || !*latest || (*latest)->sequence != 299 || decode(**latest) != 299) {
      ::_exit(16);
    }
    subscriber.release(**latest);
    ::_exit(0);
  }
  static_cast<void>(::close(registered_pipe[1]));
  static_cast<void>(::close(publish_pipe[0]));
  char registered = 0;
  ASSERT_EQ(::read(registered_pipe[0], &registered, sizeof(registered)), sizeof(registered));
  ASSERT_EQ(registered, 'r');
  for (std::uint64_t sequence = 0; sequence < 300; ++sequence) {
    ASSERT_TRUE(offer(publisher, sequence));
  }
  const char publish = 'p';
  ASSERT_EQ(::write(publish_pipe[1], &publish, sizeof(publish)), sizeof(publish));
  static_cast<void>(::close(registered_pipe[0]));
  static_cast<void>(::close(publish_pipe[1]));
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST_F(LatestSubscriptionTest, LateJoinStartsAtTheCurrentReservationBoundary) {
  auto publisher = channel_->publisher();
  for (std::uint64_t sequence = 0; sequence < 8; ++sequence) {
    ASSERT_TRUE(offer(publisher, sequence));
  }

  auto subscriber = channel_->subscriber();
  auto old_messages = subscriber.try_recv_checked();
  ASSERT_TRUE(old_messages);
  EXPECT_FALSE(*old_messages);
  EXPECT_EQ(subscriber.lagged_messages(), 0u);

  ASSERT_TRUE(offer(publisher, 8));
  auto next = subscriber.try_recv_checked();
  ASSERT_TRUE(next);
  ASSERT_TRUE(*next);
  EXPECT_EQ((*next)->sequence, 8u);
  EXPECT_EQ(decode(**next), 8u);
  subscriber.release(**next);
}

TEST_F(LatestSubscriptionTest, ReliableAndDropOldestSubscribersKeepIndependentPolicies) {
  auto publisher = channel_->publisher();
  auto reliable = channel_->subscriber(salias::SubscriptionPolicy::Reliable);
  auto lossy = channel_->subscriber(salias::SubscriptionPolicy::DropOldest);

  for (std::uint64_t sequence = 0; sequence < 300; ++sequence) {
    ASSERT_TRUE(offer(publisher, sequence));
    auto message = reliable.try_recv_checked();
    ASSERT_TRUE(message);
    ASSERT_TRUE(*message);
    EXPECT_EQ((*message)->sequence, sequence);
    EXPECT_EQ(decode(**message), sequence);
    reliable.release(**message);
  }

  EXPECT_EQ(reliable.lagged_messages(), 0u);
  auto reliable_seek = reliable.skip_to_latest();
  ASSERT_FALSE(reliable_seek);
  EXPECT_EQ(reliable_seek.error(), salias::Error::BadConfig);

  auto lagged = lossy.try_recv_checked();
  ASSERT_FALSE(lagged);
  EXPECT_EQ(lagged.error(), salias::Error::Lagged);
  EXPECT_EQ(lossy.lagged_messages(), 172u);
  auto retained = lossy.try_recv_checked();
  ASSERT_TRUE(retained);
  ASSERT_TRUE(*retained);
  EXPECT_EQ((*retained)->sequence, 172u);
  EXPECT_EQ(decode(**retained), 172u);
  lossy.release(**retained);
}

TEST_F(LatestSubscriptionTest, ReliableBatchProgressAndBorrowBlockEvictionUntilFlush) {
  auto publisher = channel_->publisher();
  auto reliable = channel_->subscriber(salias::SubscriptionPolicy::Reliable);
  auto lossy = channel_->subscriber(salias::SubscriptionPolicy::DropOldest);

  for (std::uint64_t sequence = 0; sequence < 128; ++sequence) {
    ASSERT_TRUE(offer(publisher, sequence));
  }
  const std::uint64_t boundary_sequence = 128;
  auto blocked = publisher.offer(std::as_bytes(std::span{&boundary_sequence, 1}));
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::Error::BackPressured);

  bool attempted = false;
  bool callback_write_blocked = false;
  const auto received = reliable.poll(32, [&](const salias::Message&) noexcept {
    if (attempted) {
      return;
    }
    attempted = true;
    const std::uint64_t sequence = 128;
    auto result = publisher.offer(std::as_bytes(std::span{&sequence, 1}));
    callback_write_blocked = !result && result.error() == salias::Error::BackPressured;
  });
  EXPECT_EQ(received, 32u);
  EXPECT_TRUE(callback_write_blocked);
  ASSERT_TRUE(offer(publisher, 128));

  for (std::uint64_t sequence = 32; sequence <= 128; ++sequence) {
    auto message = reliable.try_recv_checked();
    ASSERT_TRUE(message);
    ASSERT_TRUE(*message);
    EXPECT_EQ((*message)->sequence, sequence);
    EXPECT_EQ(decode(**message), sequence);
    reliable.release(**message);
  }
  EXPECT_EQ(reliable.lagged_messages(), 0u);

  auto lagged = lossy.try_recv_checked();
  ASSERT_FALSE(lagged);
  EXPECT_EQ(lagged.error(), salias::Error::Lagged);
  EXPECT_EQ(lossy.lagged_messages(), 1u);
}

TEST_F(LatestSubscriptionTest, CrossProcessReliablePolicyProtectsUnreadFrames) {
  auto publisher = channel_->publisher();
  int registered_pipe[2]{};
  int command_pipe[2]{};
  int result_pipe[2]{};
  ASSERT_EQ(::pipe(registered_pipe), 0);
  ASSERT_EQ(::pipe(command_pipe), 0);
  ASSERT_EQ(::pipe(result_pipe), 0);

  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(registered_pipe[0]));
    static_cast<void>(::close(command_pipe[1]));
    static_cast<void>(::close(result_pipe[0]));
    auto connected = Channel::connect(config_.name);
    if (!connected) {
      const char failed = 'e';
      const auto written = ::write(registered_pipe[1], &failed, sizeof(failed));
      static_cast<void>(written);
      ::_exit(10);
    }
    auto subscriber = connected->try_subscriber(salias::SubscriptionPolicy::Reliable);
    if (!subscriber) {
      const char failed = 'e';
      const auto written = ::write(registered_pipe[1], &failed, sizeof(failed));
      static_cast<void>(written);
      ::_exit(11);
    }
    const char registered = 'r';
    if (::write(registered_pipe[1], &registered, sizeof(registered)) != sizeof(registered)) {
      ::_exit(12);
    }
    char command = 0;
    if (::read(command_pipe[0], &command, sizeof(command)) != sizeof(command) || command != 'r') {
      ::_exit(0);
    }
    auto message = subscriber->try_recv_checked();
    char result = 'e';
    if (message && *message && decode(**message) == 0) {
      subscriber->release(**message);
      result = 'a';
    }
    const auto written = ::write(result_pipe[1], &result, sizeof(result));
    if (written != sizeof(result)) {
      ::_exit(14);
    }
    ::_exit(result == 'a' ? 0 : 13);
  }

  static_cast<void>(::close(registered_pipe[1]));
  static_cast<void>(::close(command_pipe[0]));
  static_cast<void>(::close(result_pipe[1]));
  char registration = 0;
  const auto registration_bytes = ::read(registered_pipe[0], &registration, sizeof(registration));
  static_cast<void>(::close(registered_pipe[0]));
  const bool child_registered = registration_bytes == sizeof(registration) && registration == 'r';

  bool prefill_ok = child_registered;
  bool backpressured = false;
  bool resumed = false;
  std::uint64_t failed_at = 0;
  std::optional<salias::LatestMpmcSubscriber> lossy;
  if (child_registered) {
    auto subscriber_result = channel_->try_subscriber(salias::SubscriptionPolicy::DropOldest);
    if (!subscriber_result) {
      prefill_ok = false;
    } else {
      lossy.emplace(std::move(*subscriber_result));
    }
    for (std::uint64_t sequence = 0; prefill_ok && sequence < 128; ++sequence) {
      if (!offer(publisher, sequence)) {
        prefill_ok = false;
      }
    }
    if (prefill_ok) {
      failed_at = 128;
      auto full = publisher.offer(std::as_bytes(std::span{&failed_at, 1}));
      backpressured = !full && full.error() == salias::Error::BackPressured;
    }
  }

  if (child_registered) {
    const char command = prefill_ok ? 'r' : 'x';
    const auto command_bytes = ::write(command_pipe[1], &command, sizeof(command));
    static_cast<void>(command_bytes);
  }
  static_cast<void>(::close(command_pipe[1]));
  char child_result = 0;
  const auto child_result_bytes = ::read(result_pipe[0], &child_result, sizeof(child_result));
  static_cast<void>(::close(result_pipe[0]));
  if (child_result_bytes == sizeof(child_result) && child_result == 'a' && backpressured) {
    resumed = offer(publisher, failed_at);
  }
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
  EXPECT_TRUE(child_registered);
  EXPECT_TRUE(prefill_ok);
  EXPECT_TRUE(backpressured);
  EXPECT_EQ(child_result, 'a');
  EXPECT_TRUE(resumed);

  if (resumed && lossy) {
    auto lagged = lossy->try_recv_checked();
    ASSERT_FALSE(lagged);
    EXPECT_EQ(lagged.error(), salias::Error::Lagged);
    EXPECT_EQ(lossy->lagged_messages(), 1u);
  }
}

void check_concurrent_delivery(salias::LatestMpmcPublisher publisher,
                               salias::LatestMpmcSubscriber subscriber, bool seek) {
  constexpr std::uint64_t kMessages = 30000;
  for (std::uint64_t sequence = 0; sequence < 300; ++sequence) {
    ASSERT_TRUE(publisher.offer(std::as_bytes(std::span{&sequence, 1})));
  }
  std::atomic<bool> stop{false};
  std::atomic<bool> failed{false};
  std::thread writer([&] {
    for (std::uint64_t sequence = 300;
         sequence < kMessages && !stop.load(std::memory_order_relaxed);) {
      auto offered = publisher.offer(std::as_bytes(std::span{&sequence, 1}));
      if (offered) {
        ++sequence;
      } else if (offered.error() == salias::Error::BackPressured) {
        std::this_thread::yield();
      } else {
        failed.store(true, std::memory_order_relaxed);
        break;
      }
    }
  });
  std::uint64_t received = 0;
  std::uint64_t next = 0;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (next < kMessages && !failed.load(std::memory_order_relaxed) &&
         std::chrono::steady_clock::now() < deadline) {
    if (seek) {
      auto skipped = subscriber.skip_to_latest();
      if (!skipped && skipped.error() != salias::Error::BackPressured) {
        failed.store(true, std::memory_order_relaxed);
        break;
      }
    }
    auto message = subscriber.try_recv_checked();
    if (!message) {
      if (message.error() != salias::Error::Lagged) {
        failed.store(true, std::memory_order_relaxed);
      }
      continue;
    }
    if (!*message) {
      std::this_thread::yield();
      continue;
    }
    const auto& item = **message;
    if (item.payload.size() != sizeof(std::uint64_t) || item.sequence < next ||
        item.sequence >= kMessages || decode(item) != item.sequence) {
      failed.store(true, std::memory_order_relaxed);
    }
    next = item.sequence + 1;
    ++received;
    std::this_thread::yield();
    if (item.payload.size() == sizeof(std::uint64_t) && decode(item) != item.sequence) {
      failed.store(true, std::memory_order_relaxed);
    }
    subscriber.release(item);
  }
  stop.store(true, std::memory_order_relaxed);
  writer.join();
  EXPECT_FALSE(failed.load(std::memory_order_relaxed));
  EXPECT_EQ(next, kMessages);
  EXPECT_EQ(received + subscriber.lagged_messages() + subscriber.skipped_messages(), kMessages);
  EXPECT_GE(subscriber.lagged_messages(), 172u);
  if (seek) {
    EXPECT_GT(subscriber.skipped_messages(), 0u);
  } else {
    EXPECT_EQ(subscriber.skipped_messages(), 0u);
  }
}

TEST_F(LatestSubscriptionTest, ConcurrentCheckedReaderAccountsForEveryPublishedFrame) {
  check_concurrent_delivery(channel_->publisher(), channel_->subscriber(), false);
}

TEST_F(LatestSubscriptionTest, ConcurrentSeekAndCheckedReceiveAccountForEveryPublishedFrame) {
  check_concurrent_delivery(channel_->publisher(), channel_->subscriber(), true);
}

}  // namespace
