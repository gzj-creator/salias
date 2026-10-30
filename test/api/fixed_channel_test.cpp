/**
 * @file test/api/fixed_channel_test.cpp
 * @brief 固定 schema 具名 Fast Ring 快速通道的拓扑、生命周期和跨进程测试。
 */
#include "salias/fixed_channel.hpp"

#include <gtest/gtest.h>
#include <sched.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace {

struct alignas(64) Payload {
  std::uint32_t producer = 0;
  std::uint32_t sequence = 0;
  std::array<std::byte, 56> body{};

  bool operator==(const Payload&) const = default;
};

static_assert(sizeof(Payload) == 64);

struct PayloadSchema {
  using message_type = Payload;
  static constexpr std::uint64_t schema_id = 0x53414C4941534649ull;
  static constexpr std::uint32_t version = 1;
};

struct OtherSchema {
  using message_type = Payload;
  static constexpr std::uint64_t schema_id = 0x53414C4941534658ull;
  static constexpr std::uint32_t version = 1;
};

struct VersionTwoSchema {
  using message_type = Payload;
  static constexpr std::uint64_t schema_id = PayloadSchema::schema_id;
  static constexpr std::uint32_t version = 2;
};

Payload make_payload(std::uint32_t producer, std::uint32_t sequence) {
  Payload payload{.producer = producer, .sequence = sequence};
  for (std::size_t index = 0; index < payload.body.size(); ++index) {
    payload.body[index] = static_cast<std::byte>((sequence + producer * 31 + index) & 0xFFu);
  }
  return payload;
}

std::string unique_name(std::string_view suffix) {
  static std::atomic<std::uint64_t> counter{0};
  return "fixed-test-" + std::to_string(::getpid()) + "-" + std::to_string(counter.fetch_add(1)) +
         "-" + std::string(suffix);
}

template <
    salias::FixedTopology Topology, class Schema, std::size_t Capacity,
    std::size_t Producers =
        (Topology == salias::FixedTopology::Spsc || Topology == salias::FixedTopology::Spmc ? 1
                                                                                            : 2),
    std::size_t Consumers =
        (Topology == salias::FixedTopology::Spsc || Topology == salias::FixedTopology::Mpsc ? 1
                                                                                            : 2)>
using Fixed = salias::FixedChannel<Topology, Schema, Capacity, Producers, Consumers>;

TEST(FixedChannelTest, SpscNamedCreateConnectSchemaAndClaimPaths) {
  const std::string name = unique_name("spsc");
  using Channel = Fixed<salias::FixedTopology::Spsc, PayloadSchema, 8>;

  auto created = Channel::create(name);
  ASSERT_TRUE(created) << static_cast<int>(created.error());
  auto duplicate = Channel::create(name);
  ASSERT_FALSE(duplicate);
  EXPECT_EQ(duplicate.error(), salias::Error::PlatformFail);

  auto connected = Channel::connect(name);
  ASSERT_TRUE(connected) << static_cast<int>(connected.error());
  auto wrong_schema = Fixed<salias::FixedTopology::Spsc, OtherSchema, 8>::connect(name);
  ASSERT_FALSE(wrong_schema);
  EXPECT_EQ(wrong_schema.error(), salias::Error::SchemaMismatch);
  auto wrong_version = Fixed<salias::FixedTopology::Spsc, VersionTwoSchema, 8>::connect(name);
  ASSERT_FALSE(wrong_version);
  EXPECT_EQ(wrong_version.error(), salias::Error::SchemaMismatch);
  auto wrong_layout = Fixed<salias::FixedTopology::Spsc, PayloadSchema, 16>::connect(name);
  ASSERT_FALSE(wrong_layout);
  EXPECT_EQ(wrong_layout.error(), salias::Error::BadConfig);

  auto publisher = created->publisher(0);
  auto subscriber = connected->subscriber(0);
  ASSERT_TRUE(publisher);
  ASSERT_TRUE(subscriber);

  for (std::uint32_t sequence = 0; sequence < Channel::capacity_slots(); ++sequence) {
    EXPECT_TRUE(publisher->send(make_payload(0, sequence)));
  }
  EXPECT_EQ(publisher->send(make_payload(0, 8)).error(), salias::Error::BackPressured);

  for (std::uint32_t sequence = 0; sequence < Channel::capacity_slots(); ++sequence) {
    auto received = subscriber->try_recv_copy();
    ASSERT_TRUE(received);
    ASSERT_TRUE(*received);
    EXPECT_EQ(**received, make_payload(0, sequence));
  }
  EXPECT_FALSE(subscriber->try_recv_copy().value());

  auto claim = publisher->try_claim();
  ASSERT_TRUE(claim);
  const Payload expected = make_payload(0, 99);
  std::memcpy(claim->payload().data(), &expected, sizeof(expected));
  ASSERT_TRUE(publisher->commit(std::move(*claim)));
  auto received = subscriber->try_recv_copy();
  ASSERT_TRUE(received);
  ASSERT_TRUE(*received);
  EXPECT_EQ(**received, expected);

  EXPECT_EQ(created->publisher(1).error(), salias::Error::BadConfig);
  EXPECT_EQ(created->subscriber(1).error(), salias::Error::BadConfig);
}

TEST(FixedChannelTest, SpmcFanoutUsesSlowestConsumerForBackpressure) {
  const std::string name = unique_name("spmc");
  using Channel = Fixed<salias::FixedTopology::Spmc, PayloadSchema, 4>;

  auto created = Channel::create(name);
  ASSERT_TRUE(created);
  auto publisher = created->publisher(0);
  auto first = created->subscriber(0);
  auto second = created->subscriber(1);
  ASSERT_TRUE(publisher);
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);

  for (std::uint32_t sequence = 0; sequence < Channel::capacity_slots(); ++sequence) {
    ASSERT_TRUE(publisher->send(make_payload(0, sequence)));
  }
  EXPECT_EQ(publisher->send(make_payload(0, 4)).error(), salias::Error::BackPressured);

  for (std::uint32_t sequence = 0; sequence < Channel::capacity_slots(); ++sequence) {
    auto received = first->try_recv_copy();
    ASSERT_TRUE(received);
    ASSERT_TRUE(*received);
    EXPECT_EQ(**received, make_payload(0, sequence));
  }
  EXPECT_EQ(publisher->send(make_payload(0, 4)).error(), salias::Error::BackPressured);

  for (std::uint32_t sequence = 0; sequence < Channel::capacity_slots(); ++sequence) {
    auto received = second->try_recv_copy();
    ASSERT_TRUE(received);
    ASSERT_TRUE(*received);
    EXPECT_EQ(**received, make_payload(0, sequence));
  }
  EXPECT_TRUE(publisher->send(make_payload(0, 4)));
  EXPECT_TRUE(first->try_recv_copy().value());
  EXPECT_TRUE(second->try_recv_copy().value());
}

TEST(FixedChannelTest, MpscAndMpmcPreservePerProducerOrder) {
  const std::string mpsc_name = unique_name("mpsc");
  using Mpsc = Fixed<salias::FixedTopology::Mpsc, PayloadSchema, 8, 2, 1>;
  auto mpsc = Mpsc::create(mpsc_name);
  ASSERT_TRUE(mpsc);
  auto first = mpsc->publisher(0);
  auto second = mpsc->publisher(1);
  auto consumer = mpsc->subscriber(0);
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  ASSERT_TRUE(consumer);

  for (std::uint32_t sequence = 0; sequence < Mpsc::capacity_slots(); ++sequence) {
    ASSERT_TRUE(first->send(make_payload(0, sequence)));
    ASSERT_TRUE(second->send(make_payload(1, sequence)));
  }
  std::array<std::uint32_t, Mpsc::producer_count()> expected{};
  for (std::size_t index = 0; index < 2 * Mpsc::capacity_slots(); ++index) {
    auto received = consumer->try_recv_copy();
    ASSERT_TRUE(received);
    ASSERT_TRUE(*received);
    ASSERT_LT((*received)->producer, Mpsc::producer_count());
    EXPECT_EQ((*received)->sequence, expected[(*received)->producer]);
    EXPECT_EQ(**received, make_payload((*received)->producer, (*received)->sequence));
    ++expected[(*received)->producer];
  }

  const std::string mpmc_name = unique_name("mpmc");
  using Mpmc = Fixed<salias::FixedTopology::Mpmc, PayloadSchema, 8, 2, 2>;
  auto mpmc = Mpmc::create(mpmc_name);
  ASSERT_TRUE(mpmc);
  auto mpmc_first = mpmc->publisher(0);
  auto mpmc_second = mpmc->publisher(1);
  auto left = mpmc->subscriber(0);
  auto right = mpmc->subscriber(1);
  ASSERT_TRUE(mpmc_first);
  ASSERT_TRUE(mpmc_second);
  ASSERT_TRUE(left);
  ASSERT_TRUE(right);

  for (std::uint32_t sequence = 0; sequence < Mpmc::capacity_slots(); ++sequence) {
    ASSERT_TRUE(mpmc_first->send(make_payload(0, sequence)));
    ASSERT_TRUE(mpmc_second->send(make_payload(1, sequence)));
  }
  expected.fill(0);
  for (std::size_t index = 0; index < 2 * Mpmc::capacity_slots(); ++index) {
    auto received = left->try_recv_copy();
    ASSERT_TRUE(received);
    ASSERT_TRUE(*received);
    ASSERT_LT((*received)->producer, Mpmc::producer_count());
    EXPECT_EQ((*received)->sequence, expected[(*received)->producer]);
    ++expected[(*received)->producer];
  }
  expected.fill(0);
  for (std::size_t index = 0; index < 2 * Mpmc::capacity_slots(); ++index) {
    auto received = right->try_recv_copy();
    ASSERT_TRUE(received);
    ASSERT_TRUE(*received);
    ASSERT_LT((*received)->producer, Mpmc::producer_count());
    EXPECT_EQ((*received)->sequence, expected[(*received)->producer]);
    ++expected[(*received)->producer];
  }
}

TEST(FixedChannelTest, CrossProcessConnectPublishesEveryFixedPayload) {
  constexpr std::uint32_t kMessages = 20'000;
  const std::string name = unique_name("cross-process");
  using Channel = Fixed<salias::FixedTopology::Spsc, PayloadSchema, 64>;

  auto created = Channel::create(name);
  ASSERT_TRUE(created);
  auto subscriber = created->subscriber(0);
  ASSERT_TRUE(subscriber);

  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    ::alarm(15);
    auto connected = Channel::connect(name);
    if (!connected) {
      ::_exit(10);
    }
    auto publisher = connected->publisher(0);
    if (!publisher) {
      ::_exit(11);
    }
    for (std::uint32_t sequence = 0; sequence < kMessages; ++sequence) {
      for (;;) {
        auto sent = publisher->send(make_payload(7, sequence));
        if (sent) {
          break;
        }
        if (sent.error() != salias::Error::BackPressured) {
          ::_exit(12);
        }
        ::sched_yield();
      }
    }
    ::_exit(0);
  }

  for (std::uint32_t sequence = 0; sequence < kMessages; ++sequence) {
    for (;;) {
      auto received = subscriber->try_recv_copy();
      ASSERT_TRUE(received);
      if (*received) {
        EXPECT_EQ(**received, make_payload(7, sequence));
        break;
      }
      ::sched_yield();
    }
  }

  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(FixedChannelTest, InvalidNamesAreRejectedWithoutCreatingSegments) {
  using Channel = Fixed<salias::FixedTopology::Spsc, PayloadSchema, 8>;
  for (const std::string_view name :
       {std::string_view{}, std::string_view{"bad/name"}, std::string_view{"bad name"}}) {
    auto created = Channel::create(name);
    ASSERT_FALSE(created);
    EXPECT_EQ(created.error(), salias::Error::BadConfig);
  }
}

TEST(FixedChannelTest, CreatorDestructionReleasesBothNamedSegments) {
  const std::string name = unique_name("lifecycle");
  using Channel = Fixed<salias::FixedTopology::Spsc, PayloadSchema, 8>;
  {
    auto created = Channel::create(name);
    ASSERT_TRUE(created);
    auto publisher = created->publisher(0);
    auto subscriber = created->subscriber(0);
    ASSERT_TRUE(publisher);
    ASSERT_TRUE(subscriber);
    ASSERT_TRUE(publisher->send(make_payload(1, 1)));
    EXPECT_TRUE(subscriber->try_recv_copy().value());
  }

  auto recreated = Channel::create(name);
  ASSERT_TRUE(recreated) << static_cast<int>(recreated.error());
}

}  // namespace
