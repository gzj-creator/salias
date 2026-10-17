/**
 * @file test/api/keyed_latest_test.cpp
 * @brief Boundary, concurrency and process-failure tests for KeyedLatestChannel.
 */
#include <gtest/gtest.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include "salias/salias.hpp"

namespace {

struct Value {
  std::uint64_t sequence = 0;
  std::uint64_t inverse = 0;
};

struct OtherValue {
  std::uint64_t first = 0;
  std::uint64_t second = 0;
};

static_assert(std::has_unique_object_representations_v<std::uint64_t>);
static_assert(std::is_trivially_copyable_v<Value>);

std::string unique_name(std::string_view tag) {
  static std::atomic<std::uint64_t> next{0};
  return "keyed-latest-" + std::to_string(::getpid()) + "-" + std::string(tag) + "-" +
         std::to_string(next.fetch_add(1, std::memory_order_relaxed));
}

template <class Subscriber>
auto recv_until(Subscriber& subscriber) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  for (;;) {
    auto result = subscriber.try_recv_checked();
    if (result && *result) {
      return std::move(**result);
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      ADD_FAILURE() << "timed out waiting for keyed latest value";
      using Message = typename Subscriber::message_type;
      return Message{};
    }
    std::this_thread::yield();
  }
}

using FourKeys = salias::KeyedLatestChannel<std::uint64_t, Value, 4>;

TEST(KeyedLatestTest, RepeatedUpdatesConflatePerSubscriberAndReturnOwnedSnapshots) {
  auto created = FourKeys::create(unique_name("conflate"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();

  ASSERT_TRUE(publisher.offer(42, Value{1, ~std::uint64_t{1}}));
  auto first = channel.subscriber();
  auto initial = first.try_recv_checked();
  ASSERT_TRUE(initial);
  ASSERT_TRUE(*initial);
  EXPECT_EQ((**initial).key, 42u);
  EXPECT_EQ((**initial).value.sequence, 1u);
  EXPECT_EQ((**initial).value.inverse, ~std::uint64_t{1});
  EXPECT_EQ((**initial).version, 1u);
  EXPECT_FALSE(first.try_recv());
  auto shared_cursor = first;
  EXPECT_FALSE(shared_cursor.try_recv());

  auto independent = channel.subscriber();
  ASSERT_TRUE(publisher.offer(42, Value{2, ~std::uint64_t{2}}));
  ASSERT_TRUE(publisher.offer(42, Value{3, ~std::uint64_t{3}}));
  auto latest = first.try_recv_checked();
  ASSERT_TRUE(latest);
  ASSERT_TRUE(*latest);
  EXPECT_EQ((**latest).value.sequence, 3u);
  EXPECT_EQ((**latest).version, 3u);
  EXPECT_FALSE(shared_cursor.try_recv());
  EXPECT_EQ(first.conflated_updates(), 1u);
  EXPECT_EQ(independent.conflated_updates(), 0u);

  ASSERT_TRUE(publisher.offer(42, Value{4, ~std::uint64_t{4}}));
  auto copied_snapshot = first.try_recv();
  ASSERT_TRUE(copied_snapshot);
  EXPECT_EQ(copied_snapshot->value.sequence, 4u);
  EXPECT_EQ((**latest).value.sequence, 3u);
  auto independent_snapshot = independent.try_recv();
  ASSERT_TRUE(independent_snapshot);
  EXPECT_EQ(independent_snapshot->value.sequence, 4u);
  EXPECT_EQ(independent.conflated_updates(), 3u);
  auto late_joiner = channel.subscriber();
  auto current = late_joiner.try_recv();
  ASSERT_TRUE(current);
  EXPECT_EQ(current->value.sequence, 4u);
  EXPECT_EQ(late_joiner.conflated_updates(), 0u);
}

TEST(KeyedLatestTest, ExactHashCollisionsAndCapacityExhaustionAreHandled) {
  using Tiny = salias::KeyedLatestChannel<std::uint64_t, std::uint64_t, 2>;
  auto created = Tiny::create(unique_name("collisions"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();

  std::uint64_t first_key = 0;
  std::uint64_t second_key = 0;
  bool found_collision = false;
  for (std::uint64_t left = 0; left < 100 && !found_collision; ++left) {
    const auto left_bytes = salias::keyed_latest_detail::to_bytes(left);
    const auto left_home = salias::keyed_latest_detail::hash_key(left_bytes) % Tiny::capacity();
    for (std::uint64_t right = left + 1; right < 100; ++right) {
      const auto right_bytes = salias::keyed_latest_detail::to_bytes(right);
      if (salias::keyed_latest_detail::hash_key(right_bytes) % Tiny::capacity() == left_home) {
        first_key = left;
        second_key = right;
        found_collision = true;
        break;
      }
    }
  }
  ASSERT_TRUE(found_collision);
  ASSERT_TRUE(publisher.offer(first_key, 10));
  ASSERT_TRUE(publisher.offer(second_key, 20));
  ASSERT_TRUE(publisher.offer(first_key, 11));

  auto subscriber = channel.subscriber();
  auto first = subscriber.try_recv();
  ASSERT_TRUE(first);
  auto second = subscriber.try_recv();
  ASSERT_TRUE(second);
  EXPECT_NE(first->key, second->key);
  EXPECT_EQ(first->key == first_key ? first->value : second->value, 11u);
  EXPECT_EQ(first->key == second_key ? first->value : second->value, 20u);
  EXPECT_FALSE(subscriber.try_recv());

  const auto full = publisher.offer(9999, 99);
  ASSERT_FALSE(full);
  EXPECT_EQ(full.error(), salias::Error::CapacityExceeded);
  ASSERT_TRUE(publisher.offer(second_key, 21));
}

TEST(KeyedLatestTest, EventLogOverrunFallsBackToTheCurrentPerKeySnapshot) {
  using OneKey = salias::KeyedLatestChannel<std::uint64_t, std::uint64_t, 1>;
  auto created = OneKey::create(unique_name("event-overrun"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  for (std::uint64_t sequence = 1; sequence <= 200; ++sequence) {
    ASSERT_TRUE(publisher.offer(9, sequence));
  }

  auto latest = subscriber.try_recv_checked();
  ASSERT_TRUE(latest);
  ASSERT_TRUE(*latest);
  EXPECT_EQ((**latest).key, 9u);
  EXPECT_EQ((**latest).value, 200u);
  EXPECT_EQ((**latest).version, 200u);
  EXPECT_EQ(subscriber.conflated_updates(), 199u);
  EXPECT_FALSE(subscriber.try_recv());
}

TEST(KeyedLatestTest, ConnectChecksStaticCapacityAndSchema) {
  const auto name = unique_name("schema");
  auto created = FourKeys::create(name);
  ASSERT_TRUE(created);
  auto wrong_capacity = salias::KeyedLatestChannel<std::uint64_t, Value, 8>::connect(name);
  EXPECT_FALSE(wrong_capacity);

  using DifferentValue = salias::KeyedLatestChannel<std::uint64_t, OtherValue, 4>;
  auto wrong_value = DifferentValue::connect(name);
  EXPECT_FALSE(wrong_value);
  EXPECT_EQ(wrong_value.error(), salias::Error::BadConfig);
  auto connected = FourKeys::connect(name);
  EXPECT_TRUE(connected);

  auto missing = FourKeys::connect(unique_name("missing"));
  ASSERT_FALSE(missing);
  EXPECT_EQ(missing.error(), salias::Error::NotFound);
  EXPECT_FALSE(FourKeys::create("bad/name"));
  EXPECT_FALSE(FourKeys::create(""));
  EXPECT_FALSE(FourKeys::create(name));
}

TEST(KeyedLatestTest, InvalidEndpointsAndExhaustedPerKeyVersionReturnTypedErrors) {
  salias::KeyedLatestPublisher<std::uint64_t, std::uint64_t> invalid_publisher;
  auto invalid_offer = invalid_publisher.offer(1, 2);
  ASSERT_FALSE(invalid_offer);
  EXPECT_EQ(invalid_offer.error(), salias::Error::BadConfig);

  salias::KeyedLatestSubscriber<std::uint64_t, std::uint64_t, 1> invalid_subscriber;
  auto invalid_receive = invalid_subscriber.try_recv_checked();
  ASSERT_FALSE(invalid_receive);
  EXPECT_EQ(invalid_receive.error(), salias::Error::BadConfig);

  using OneKey = salias::KeyedLatestChannel<std::uint64_t, std::uint64_t, 1>;
  const auto name = unique_name("version-limit");
  auto created = OneKey::create(name);
  ASSERT_TRUE(created);
  auto publisher = created->publisher();
  ASSERT_TRUE(publisher.offer(5, 10));
  auto storage = salias::detail::connect_keyed_latest_storage(
      name, OneKey::capacity(), sizeof(std::uint64_t), sizeof(std::uint64_t),
      salias::keyed_latest_detail::type_schema<std::uint64_t>(),
      salias::keyed_latest_detail::type_schema<std::uint64_t>());
  ASSERT_TRUE(storage);
  const auto set_max_version = [](salias::detail::KeyedLatestTableView table,
                                  void* context) noexcept {
    const auto key = *static_cast<const std::uint64_t*>(context);
    for (std::size_t index = 0; index < table.slot_count; ++index) {
      auto slot = table.slot(index);
      if (std::memcmp(slot.key, &key, sizeof(key)) != 0) {
        continue;
      }
      const auto active =
          std::atomic_ref<std::uint64_t>(*slot.commit).load(std::memory_order_acquire) & 2u;
      std::atomic_ref<std::uint64_t>(*slot.commit)
          .store(salias::keyed_latest_detail::make_commit(salias::keyed_latest_detail::kMaxVersion,
                                                          active >> 1),
                 std::memory_order_release);
      return salias::Error::Ok;
    }
    return salias::Error::NotFound;
  };
  std::uint64_t key = 5;
  ASSERT_EQ(salias::detail::with_keyed_latest_table(*storage, set_max_version, &key, true),
            salias::Error::Ok);
  auto exhausted = publisher.offer(key, 11);
  ASSERT_FALSE(exhausted);
  EXPECT_EQ(exhausted.error(), salias::Error::SequenceExhausted);
}

TEST(KeyedLatestTest, ExhaustedEventSequenceRejectsOfferWithoutChangingSnapshot) {
  using OneKey = salias::KeyedLatestChannel<std::uint64_t, std::uint64_t, 1>;
  const auto name = unique_name("event-limit");
  auto created = OneKey::create(name);
  ASSERT_TRUE(created);
  auto publisher = created->publisher();
  ASSERT_TRUE(publisher.offer(5, 10));

  auto storage = salias::detail::connect_keyed_latest_storage(
      name, OneKey::capacity(), sizeof(std::uint64_t), sizeof(std::uint64_t),
      salias::keyed_latest_detail::type_schema<std::uint64_t>(),
      salias::keyed_latest_detail::type_schema<std::uint64_t>());
  ASSERT_TRUE(storage);
  const auto exhaust_events = [](salias::detail::KeyedLatestTableView table, void*) noexcept {
    std::atomic_ref<std::uint64_t>(*table.event_sequence)
        .store(std::numeric_limits<std::uint64_t>::max(), std::memory_order_release);
    return salias::Error::Ok;
  };
  ASSERT_EQ(salias::detail::with_keyed_latest_table(*storage, exhaust_events, nullptr, true),
            salias::Error::Ok);

  auto exhausted = publisher.offer(5, 11);
  ASSERT_FALSE(exhausted);
  EXPECT_EQ(exhausted.error(), salias::Error::SequenceExhausted);
  auto subscriber = created->subscriber();
  auto snapshot = subscriber.try_recv_checked();
  ASSERT_TRUE(snapshot);
  ASSERT_TRUE(*snapshot);
  EXPECT_EQ((**snapshot).key, 5u);
  EXPECT_EQ((**snapshot).value, 10u);
  EXPECT_EQ((**snapshot).version, 1u);
}

TEST(KeyedLatestTest, IndependentProcessPublishesIntoTheSharedKeyTable) {
  const auto name = unique_name("ipc");
  auto created = FourKeys::create(name);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto subscriber = channel.subscriber();
  ASSERT_TRUE(channel.publisher().offer(7, Value{1, ~std::uint64_t{1}}));
  auto initial = subscriber.try_recv();
  ASSERT_TRUE(initial);
  ASSERT_EQ(initial->value.sequence, 1u);

  int ready_pipe[2]{};
  ASSERT_EQ(::pipe(ready_pipe), 0);
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(ready_pipe[0]));
    auto connected = FourKeys::connect(name);
    if (!connected) {
      const auto written = ::write(ready_pipe[1], "e", 1);
      static_cast<void>(written);
      ::_exit(2);
    }
    auto offered = connected->publisher().offer(7, Value{2, ~std::uint64_t{2}});
    const char result = offered ? 'o' : 'e';
    const auto written = ::write(ready_pipe[1], &result, 1);
    static_cast<void>(written);
    ::_exit(offered ? 0 : 3);
  }

  static_cast<void>(::close(ready_pipe[1]));
  char result = 0;
  ASSERT_EQ(::read(ready_pipe[0], &result, 1), 1);
  static_cast<void>(::close(ready_pipe[0]));
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);
  ASSERT_EQ(result, 'o');

  auto latest = recv_until(subscriber);
  EXPECT_EQ(latest.key, 7u);
  EXPECT_EQ(latest.value.sequence, 2u);
  EXPECT_EQ(latest.version, 2u);
}

TEST(KeyedLatestTest, ForkInheritedPublisherGetsAProcessPrivateLockDescriptor) {
  struct LargeValue {
    std::uint64_t sequence = 0;
    std::array<std::byte, 4088> fill{};
  };
  using LargeChannel = salias::KeyedLatestChannel<std::uint64_t, LargeValue, 1>;
  auto created = LargeChannel::create(unique_name("fork-inherited"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  int start_pipe[2]{};
  int done_pipe[2]{};
  ASSERT_EQ(::pipe(start_pipe), 0);
  ASSERT_EQ(::pipe(done_pipe), 0);
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(start_pipe[1]));
    static_cast<void>(::close(done_pipe[0]));
    char start = 0;
    const auto read = ::read(start_pipe[0], &start, 1);
    if (read != 1) {
      ::_exit(5);
    }
    for (std::uint64_t index = 0; index < 500; ++index) {
      LargeValue value{};
      value.sequence = (std::uint64_t{1} << 63) | index;
      value.fill.fill(std::byte{0x55});
      for (;;) {
        auto offered = publisher.offer(1, value);
        if (offered) {
          break;
        }
        if (offered.error() != salias::Error::BackPressured) {
          ::_exit(6);
        }
        std::this_thread::yield();
      }
    }
    const char done = 'd';
    const auto written = ::write(done_pipe[1], &done, 1);
    static_cast<void>(written);
    ::_exit(0);
  }

  static_cast<void>(::close(start_pipe[0]));
  static_cast<void>(::close(done_pipe[1]));
  const char start = 's';
  ASSERT_EQ(::write(start_pipe[1], &start, 1), 1);
  for (std::uint64_t index = 0; index < 500; ++index) {
    LargeValue value{};
    value.sequence = index;
    value.fill.fill(std::byte{0xaa});
    for (;;) {
      auto offered = publisher.offer(1, value);
      if (offered) {
        break;
      }
      ASSERT_EQ(offered.error(), salias::Error::BackPressured);
      std::this_thread::yield();
    }
  }
  char done = 0;
  ASSERT_EQ(::read(done_pipe[0], &done, 1), 1);
  EXPECT_EQ(done, 'd');
  static_cast<void>(::close(start_pipe[1]));
  static_cast<void>(::close(done_pipe[0]));
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);

  auto final = subscriber.try_recv_checked();
  ASSERT_TRUE(final);
  ASSERT_TRUE(*final);
  EXPECT_EQ((**final).version, 1000u);
  const auto fill = ((**final).value.sequence >> 63) != 0 ? std::byte{0x55} : std::byte{0xaa};
  EXPECT_TRUE(std::all_of((**final).value.fill.begin(), (**final).value.fill.end(),
                          [fill](std::byte byte) { return byte == fill; }));
}

TEST(KeyedLatestTest, ConcurrentWritersAndReadersObserveWholeValues) {
  using OneKey = salias::KeyedLatestChannel<std::uint64_t, Value, 1>;
  const auto name = unique_name("concurrent");
  auto created = OneKey::create(name);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto connected = OneKey::connect(name);
  ASSERT_TRUE(connected);
  auto subscriber = channel.subscriber();
  constexpr std::uint32_t kWriterCount = 4;
  constexpr std::uint32_t kUpdatesPerWriter = 1500;
  std::atomic<std::uint32_t> writers_done{0};
  std::atomic<bool> invalid_value{false};
  std::vector<std::thread> writers;
  writers.reserve(kWriterCount);

  for (std::uint32_t writer = 0; writer < kWriterCount; ++writer) {
    auto publisher = (writer % 2 == 0) ? channel.publisher() : connected->publisher();
    writers.emplace_back([publisher = std::move(publisher), writer, &writers_done]() mutable {
      for (std::uint32_t index = 0; index < kUpdatesPerWriter; ++index) {
        const std::uint64_t sequence = (static_cast<std::uint64_t>(writer) << 32) | index;
        for (;;) {
          auto offered = publisher.offer(1, Value{sequence, ~sequence});
          if (offered) {
            break;
          }
          if (offered.error() != salias::Error::BackPressured) {
            ADD_FAILURE() << "unexpected publisher error";
            writers_done.fetch_add(1, std::memory_order_release);
            return;
          }
          std::this_thread::yield();
        }
      }
      writers_done.fetch_add(1, std::memory_order_release);
    });
  }

  while (writers_done.load(std::memory_order_acquire) != kWriterCount) {
    auto received = subscriber.try_recv_checked();
    if (received && *received && (**received).value.inverse != ~(**received).value.sequence) {
      invalid_value.store(true, std::memory_order_release);
    }
    if (!received && received.error() != salias::Error::BackPressured) {
      invalid_value.store(true, std::memory_order_release);
    }
    std::this_thread::yield();
  }
  for (auto& writer : writers) {
    writer.join();
  }
  for (std::uint32_t attempt = 0; attempt < 10000; ++attempt) {
    auto received = subscriber.try_recv_checked();
    if (received && *received) {
      EXPECT_EQ((**received).value.inverse, ~(**received).value.sequence);
      break;
    }
    std::this_thread::yield();
  }
  EXPECT_FALSE(invalid_value.load(std::memory_order_acquire));
}

struct PartialWriteContext {
  int ready_fd = -1;
  int wait_fd = -1;
  std::uint64_t key = 0;
};

salias::Error leave_partial_value(salias::detail::KeyedLatestTableView table,
                                  void* opaque) noexcept {
  auto& context = *static_cast<PartialWriteContext*>(opaque);
  for (std::size_t index = 0; index < table.slot_count; ++index) {
    auto slot = table.slot(index);
    const auto commit =
        std::atomic_ref<std::uint64_t>(*slot.commit).load(std::memory_order_acquire);
    if ((commit & 1u) == 0 || std::memcmp(slot.key, &context.key, sizeof(context.key)) != 0) {
      continue;
    }
    const std::uint32_t inactive = salias::keyed_latest_detail::active_buffer_of(commit) ^ 1u;
    constexpr std::uint64_t partial = 0xdeadbeefcafebabeull;
    std::memcpy(slot.values[inactive], &partial, sizeof(partial) / 2);
    const char ready = 'w';
    const auto written = ::write(context.ready_fd, &ready, 1);
    static_cast<void>(written);
    char wait = 0;
    const auto read = ::read(context.wait_fd, &wait, 1);
    static_cast<void>(read);
    return salias::Error::Ok;
  }
  return salias::Error::NotFound;
}

TEST(KeyedLatestTest, KilledWriterCannotExposePartiallyWrittenValueAndReleasesLock) {
  const auto name = unique_name("writer-crash");
  auto created = FourKeys::create(name);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  ASSERT_TRUE(publisher.offer(55, Value{100, ~std::uint64_t{100}}));
  auto original = subscriber.try_recv();
  ASSERT_TRUE(original);
  ASSERT_EQ(original->value.sequence, 100u);

  int ready_pipe[2]{};
  int wait_pipe[2]{};
  ASSERT_EQ(::pipe(ready_pipe), 0);
  ASSERT_EQ(::pipe(wait_pipe), 0);
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(ready_pipe[0]));
    static_cast<void>(::close(wait_pipe[1]));
    auto storage = salias::detail::connect_keyed_latest_storage(
        name, FourKeys::capacity(), sizeof(std::uint64_t), sizeof(Value),
        salias::keyed_latest_detail::type_schema<std::uint64_t>(),
        salias::keyed_latest_detail::type_schema<Value>());
    if (!storage) {
      ::_exit(4);
    }
    PartialWriteContext context{ready_pipe[1], wait_pipe[0], 55};
    static_cast<void>(
        salias::detail::with_keyed_latest_table(*storage, &leave_partial_value, &context, true));
    ::_exit(0);
  }

  static_cast<void>(::close(ready_pipe[1]));
  static_cast<void>(::close(wait_pipe[0]));
  char ready = 0;
  ASSERT_EQ(::read(ready_pipe[0], &ready, 1), 1);
  ASSERT_EQ(ready, 'w');
  auto blocked_offer = publisher.offer(55, Value{102, ~std::uint64_t{102}});
  ASSERT_FALSE(blocked_offer);
  EXPECT_EQ(blocked_offer.error(), salias::Error::BackPressured);
  auto blocked_receive = subscriber.try_recv_checked();
  ASSERT_FALSE(blocked_receive);
  EXPECT_EQ(blocked_receive.error(), salias::Error::BackPressured);
  auto blocked_registration = channel.try_subscriber();
  ASSERT_FALSE(blocked_registration);
  EXPECT_EQ(blocked_registration.error(), salias::Error::BackPressured);
  ASSERT_EQ(::kill(child, SIGKILL), 0);
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  EXPECT_TRUE(WIFSIGNALED(status));
  EXPECT_EQ(WTERMSIG(status), SIGKILL);
  static_cast<void>(::close(ready_pipe[0]));
  static_cast<void>(::close(wait_pipe[1]));

  auto after_crash = subscriber.try_recv_checked();
  ASSERT_TRUE(after_crash);
  EXPECT_FALSE(*after_crash);
  ASSERT_TRUE(publisher.offer(55, Value{101, ~std::uint64_t{101}}));
  auto recovered = subscriber.try_recv();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered->value.sequence, 101u);
  EXPECT_EQ(recovered->value.inverse, ~std::uint64_t{101});
}

}  // namespace
