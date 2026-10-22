/**
 * @file test/api/claim_api_v4_test.cpp
 * @brief v4 零拷贝 claim 接口的提交、移动与归属契约测试。
 * @details 覆盖 raw/schema-bound 两条路径、移动后提交、跨 Publisher 拒绝以及
 *          tried claim 未发布时的可见性。claim 不持有发布端，测试始终让 Publisher
 *          覆盖 claim 的完整生命周期。
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

#include <salias/salias.hpp>

namespace {

struct Payload {
  std::uint32_t value = 0;
};

struct PayloadSchema {
  using message_type = Payload;
  static constexpr std::uint64_t schema_id = 0x7634636C61696D31ull;
  static constexpr std::uint32_t version = 1;
};

salias::Config config_for(std::string_view suffix) {
  salias::Config config;
  config.name = "claim-v4-" + std::string(suffix) + "-" + std::to_string(::getpid());
  config.capacity = 1u << 20;
  return config;
}

}  // namespace

TEST(ClaimApiV4Test, RawClaimCommitsThroughPublisher) {
  auto created = salias::FifoMpscChannel::create(config_for("raw"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto claimed = publisher.try_claim(sizeof(Payload));
  ASSERT_TRUE(claimed);
  const Payload expected{.value = 0x12345678};
  std::memcpy(claimed->payload().data(), &expected, sizeof(expected));

  EXPECT_FALSE(subscriber.try_recv());
  publisher.commit(std::move(*claimed));

  auto received = subscriber.try_recv();
  ASSERT_TRUE(received);
  Payload actual{};
  std::memcpy(&actual, received->payload.data(), sizeof(actual));
  EXPECT_EQ(actual.value, expected.value);
  subscriber.release(*received);
}

TEST(ClaimApiV4Test, MovedClaimRetainsOwnershipAndCanBeCommitted) {
  auto created = salias::FifoMpscChannel::create(config_for("moved"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto claimed = publisher.try_claim(sizeof(Payload));
  ASSERT_TRUE(claimed);
  auto moved = std::move(*claimed);
  EXPECT_TRUE(claimed->payload().empty());

  const Payload expected{.value = 77};
  std::memcpy(moved.payload().data(), &expected, sizeof(expected));
  publisher.commit(std::move(moved));
  EXPECT_TRUE(moved.payload().empty());

  auto received = subscriber.try_recv();
  ASSERT_TRUE(received);
  Payload actual{};
  std::memcpy(&actual, received->payload.data(), sizeof(actual));
  EXPECT_EQ(actual.value, expected.value);
  subscriber.release(*received);
}

TEST(ClaimApiV4Test, SchemaBoundClaimCommitsThroughTypedPublisher) {
  using Channel = salias::Channel<salias::Mode::FifoMpsc, PayloadSchema>;
  auto created = Channel::create(config_for("typed"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto claimed = publisher.try_claim();
  ASSERT_TRUE(claimed);
  const Payload expected{.value = 991};
  std::memcpy(claimed->payload().data(), &expected, sizeof(expected));
  publisher.commit(std::move(*claimed));

  auto received = subscriber.recv_copy();
  ASSERT_TRUE(received);
  EXPECT_EQ(received->value, expected.value);
}

TEST(ClaimApiV4Test, SamePublisherCopyCanCommitClaim) {
  auto created = salias::FifoMpscChannel::create(config_for("copy"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto copy = publisher;
  auto subscriber = channel.subscriber();

  auto claimed = publisher.try_claim(sizeof(Payload));
  ASSERT_TRUE(claimed);
  const Payload expected{.value = 5};
  std::memcpy(claimed->payload().data(), &expected, sizeof(expected));
  copy.commit(std::move(*claimed));

  auto received = subscriber.try_recv();
  ASSERT_TRUE(received);
  Payload actual{};
  std::memcpy(&actual, received->payload.data(), sizeof(actual));
  EXPECT_EQ(actual.value, expected.value);
  subscriber.release(*received);
}

TEST(ClaimApiV4Test, CrossPublisherCommitIsRejectedWithoutPublishing) {
#if defined(NDEBUG)
  auto created = salias::FifoMpscChannel::create(config_for("cross"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto first = channel.publisher();
  auto second = channel.publisher();
  auto subscriber = channel.subscriber();

  auto claimed = first.try_claim(sizeof(Payload));
  ASSERT_TRUE(claimed);
  const Payload expected{.value = 1};
  std::memcpy(claimed->payload().data(), &expected, sizeof(expected));
  second.commit(std::move(*claimed));

  EXPECT_FALSE(subscriber.try_recv());
#else
  GTEST_SKIP() << "cross-publisher commit asserts in debug builds";
#endif
}

TEST(ClaimApiV4Test, DuplicateCommitIsIgnored) {
  auto created = salias::FifoMpscChannel::create(config_for("duplicate"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto claimed = publisher.try_claim(sizeof(Payload));
  ASSERT_TRUE(claimed);
  const Payload expected{.value = 19};
  std::memcpy(claimed->payload().data(), &expected, sizeof(expected));

  publisher.commit(std::move(*claimed));
  publisher.commit(std::move(*claimed));
  auto received = subscriber.try_recv();
  ASSERT_TRUE(received);
  Payload actual{};
  std::memcpy(&actual, received->payload.data(), sizeof(actual));
  EXPECT_EQ(actual.value, expected.value);
  subscriber.release(*received);
  EXPECT_FALSE(subscriber.try_recv());
}

TEST(ClaimApiV4Test, MovedFromClaimCannotBeCommitted) {
  auto created = salias::FifoMpscChannel::create(config_for("moved-from"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto claimed = publisher.try_claim(sizeof(Payload));
  ASSERT_TRUE(claimed);
  auto moved = std::move(*claimed);
  EXPECT_TRUE(claimed->payload().empty());

  publisher.commit(std::move(*claimed));
  EXPECT_FALSE(subscriber.try_recv());

  const Payload expected{.value = 23};
  std::memcpy(moved.payload().data(), &expected, sizeof(expected));
  publisher.commit(std::move(moved));
  auto received = subscriber.try_recv();
  ASSERT_TRUE(received);
  Payload actual{};
  std::memcpy(&actual, received->payload.data(), sizeof(actual));
  EXPECT_EQ(actual.value, expected.value);
  subscriber.release(*received);
}
