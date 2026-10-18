/** @file test/api/channel_api_test.cpp
 * @brief salias L7 公共 API（channel 层）的端到端集成测试。
 * @details 本文件位于 L5 channel / L7 salias 公共 API 层，验证各类 channel
 *   （FifoMpsc/OrderedMpsc/FifoMpmc/OrderedMpmc/LatestMpmc）在 claim/commit、offer、
 *   offer_batch、subscriber 轮询与 release 等关键路径上的正确性。覆盖面包括：
 *   FIFO 与全局有序两种交付语义、fanout 多消费者广播、生产者/消费者槽位
 *   耗尽与大页对齐校验，以及多进程（fork）共享 producer 槽位的 IPC 场景。
 *   线程/进程模型：单进程内通过 subscriber/publisher 对象操作，跨进程场景
 *   通过 fork 子进程 connect 同名共享内存区域实现。
 */

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
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

#include "core/platform/monotonic_clock.hpp"
#include "salias/salias.hpp"

namespace {  // 匿名命名空间：测试内部辅助类型与函数，仅本编译单元可见。

// 编译期断言：PublishClaim 不可拷贝（独占 claim 的所有权只能移动，防止误用）。
static_assert(!std::is_copy_constructible_v<salias::PublishClaim<salias::Mode::FifoMpsc>>);
// 编译期断言：FIFO 与 Ordered 是两种独立 channel 类型，不可互换。
static_assert(!std::is_same_v<salias::FifoMpscChannel, salias::OrderedMpscChannel>);
static_assert(!std::is_same_v<salias::FifoMpmcChannel, salias::OrderedMpmcChannel>);
static_assert(static_cast<std::uint32_t>(salias::Mode::FifoMpsc) == 0);
static_assert(static_cast<std::uint32_t>(salias::Mode::FifoMpmc) == 1);
static_assert(static_cast<std::uint32_t>(salias::Mode::OrderedMpsc) == 2);
static_assert(static_cast<std::uint32_t>(salias::Mode::OrderedMpmc) == 3);
static_assert(static_cast<std::uint32_t>(salias::Mode::LatestMpmc) == 4);

/// @brief 测试用负载结构体。
/// @details 模拟一条业务消息：producer 标识来源生产者编号，sequence 标识
///   消息序号，用于在测试中校验交付顺序与来源正确性。
struct Payload {
  std::uint32_t producer = 0;
  std::uint32_t sequence = 0;
  bool operator==(const Payload&) const = default;
};

struct PayloadSchema {
  using message_type = Payload;
  static constexpr std::uint64_t schema_id = 0x53414C4941530001ull;
  static constexpr std::uint32_t version = 1;
};

struct PayloadSchemaV2 {
  using message_type = Payload;
  static constexpr std::uint64_t schema_id = PayloadSchema::schema_id;
  static constexpr std::uint32_t version = 2;
};

struct WiderPayload {
  std::uint32_t first = 0;
  std::uint32_t second = 0;
  std::uint32_t third = 0;
};

struct WiderPayloadSchema {
  using message_type = WiderPayload;
  static constexpr std::uint64_t schema_id = PayloadSchema::schema_id;
  static constexpr std::uint32_t version = PayloadSchema::version;
};

struct OtherPayloadSchema {
  using message_type = Payload;
  static constexpr std::uint64_t schema_id = 0x53414C4941530002ull;
  static constexpr std::uint32_t version = 1;
};

using TypedFifoChannel = salias::Channel<salias::Mode::FifoMpsc, PayloadSchema>;
using TypedFifoPublisher = salias::Publisher<salias::Mode::FifoMpsc, PayloadSchema>;
using TypedFifoSubscriber = salias::Subscriber<salias::Mode::FifoMpsc, PayloadSchema>;

struct NonTrivialPayload {
  NonTrivialPayload() = default;
  NonTrivialPayload(const NonTrivialPayload&) {}
};

template <class T>
concept CanSendTyped =
    requires(salias::FifoMpscPublisher& publisher, const T& value) { publisher.send(value); };

template <class T>
concept CanReceiveTyped =
    requires(salias::FifoMpscSubscriber& subscriber) { subscriber.template try_recv_copy<T>(); };

template <class T>
concept HasBorrowedReceive = requires(T& subscriber) { subscriber.try_recv(); };

template <class T>
concept HasDynamicOffer =
    requires(T& publisher, std::span<const std::byte> bytes) { publisher.offer(bytes); };

template <class T>
concept HasDynamicClaim = requires(T& publisher) { publisher.try_claim(std::size_t{1}); };

template <class PublisherType, class Value>
concept CanSendValue =
    requires(PublisherType& publisher, const Value& value) { publisher.send(value); };

static_assert(CanSendTyped<Payload>);
static_assert(!CanSendTyped<NonTrivialPayload>);
static_assert(!salias::detail::WireMessage<int&>);
static_assert(!CanSendTyped<int*>);
static_assert(!CanSendTyped<int Payload::*>);
static_assert(!CanSendTyped<std::span<const std::byte>>);
static_assert(!CanSendTyped<std::string_view>);
static_assert(CanReceiveTyped<Payload>);
static_assert(!CanReceiveTyped<NonTrivialPayload>);
static_assert(!CanReceiveTyped<int*>);
static_assert(!CanReceiveTyped<int Payload::*>);
static_assert(!CanReceiveTyped<std::span<const std::byte>>);
static_assert(!CanReceiveTyped<std::string_view>);
static_assert(salias::detail::WireSchema<PayloadSchema>);
static_assert(!HasBorrowedReceive<TypedFifoSubscriber>);
static_assert(!HasDynamicOffer<TypedFifoPublisher>);
static_assert(!HasDynamicClaim<TypedFifoPublisher>);
static_assert(CanSendValue<TypedFifoPublisher, Payload>);
static_assert(!CanSendValue<TypedFifoPublisher, WiderPayload>);
static_assert(requires(TypedFifoSubscriber& subscriber) { subscriber.try_recv_copy(); });

/// @brief 生成进程内唯一的共享内存名称。
/// @param suffix 附加在名称末尾的可读后缀，便于区分不同用例。
/// @return 形如 "channel-api-<pid>-<counter>-<suffix>" 的唯一字符串。
/// @note 结合 PID 与原子自增计数器，保证并行测试间名称不冲突。
std::string unique_name(std::string_view suffix) {
  static std::atomic<std::uint32_t> counter = 0;
  return "channel-api-" + std::to_string(::getpid()) + "-" +
         std::to_string(counter.fetch_add(1, std::memory_order_relaxed)) + "-" +
         std::string(suffix);
}

/// @brief 构造一个最小可用的 channel 配置。
/// @param mode channel 工作模式（FIFO/Ordered × Mpsc/Fanout）。
/// @param suffix 用于生成唯一名称的后缀。
/// @return 默认 capacity 4096、单 producer 单 consumer 的 Config。
/// @note 调用方可在返回值上覆盖 num_producers/num_consumers/huge 等字段。
salias::Config config_for(salias::Mode mode, std::string_view suffix) {
  return salias::Config{
      .name = unique_name(suffix),
      .mode = mode,
      .capacity = 4096,
      .num_producers = 1,
      .num_consumers = 1,
  };
}

/// @brief 将 Payload 序列化为定长字节数组（平凡内存拷贝）。
/// @param payload 待编码的负载。
/// @return 与 Payload 等长的 std::array<std::byte>。
std::array<std::byte, sizeof(Payload)> encode(Payload payload) {
  std::array<std::byte, sizeof(Payload)> bytes{};
  std::memcpy(bytes.data(), &payload, sizeof(payload));
  return bytes;
}

/// @brief 将字节切片反序列化为 Payload。
/// @param payload 指向 frame 内 payload 区域的只读切片。
/// @return 还原后的 Payload 值。
Payload decode(std::span<const std::byte> payload) {
  Payload value{};
  std::memcpy(&value, payload.data(), sizeof(value));
  return value;
}

/// @brief 在多进程（fork）场景下轮询接收一条消息。
/// @tparam M channel 模式。
/// @param subscriber 订阅者引用。
/// @return 收到的第一条消息；若超时（10 万次 yield 后仍无）返回空 payload。
/// @note 使用 yield 自旋等待，避免引入 sleep；用于 fork 子进程发布后的同步。
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

TEST(ChannelApiTest, TypedSendAndCopyReceiveRoundTripWithoutBorrowedRelease) {
  auto config = config_for(salias::Mode::FifoMpsc, "typed-roundtrip");
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  const Payload expected{.producer = 7, .sequence = 42};

  ASSERT_TRUE(publisher.send(expected));
  auto received = subscriber.try_recv_copy<Payload>();
  ASSERT_TRUE(received);
  ASSERT_TRUE(*received);
  EXPECT_EQ(**received, expected);
  EXPECT_FALSE(subscriber.try_recv());

  auto negative_ttl = publisher.send_for(expected, std::chrono::nanoseconds{-1});
  ASSERT_FALSE(negative_ttl);
  EXPECT_EQ(negative_ttl.error(), salias::Error::BadConfig);

  ASSERT_TRUE(publisher.send_for(expected, std::chrono::nanoseconds{0}));
  auto expired = subscriber.try_recv_copy<Payload>();
  ASSERT_TRUE(expired);
  EXPECT_FALSE(*expired);
  EXPECT_EQ(subscriber.expired_messages(), 1u);

  ASSERT_TRUE(publisher.send_for(expected, std::chrono::seconds{1}));
  auto blocking = subscriber.recv_copy<Payload>();
  ASSERT_TRUE(blocking);
  EXPECT_EQ(*blocking, expected);
}

TEST(ChannelApiTest, BlockingRawReceiveFindsDelayedPublicationBeforeAdaptiveActivation) {
  auto config = config_for(salias::Mode::FifoMpsc, "blocking-raw-unactivated");
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  const Payload expected{.producer = 3, .sequence = 91};
  const auto bytes = encode(expected);

  std::atomic<bool> published{false};
  std::thread delayed_publisher([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    const auto offered = publisher.offer(bytes);
    published.store(offered && *offered, std::memory_order_release);
  });
  const auto message = subscriber.recv();
  delayed_publisher.join();

  ASSERT_TRUE(published.load(std::memory_order_acquire));
  ASSERT_EQ(message.payload.size(), sizeof(Payload));
  EXPECT_EQ(decode(message.payload), expected);
  subscriber.release(message);
}

TEST(ChannelApiTest, BlockingFixedReceiveFindsDelayedPublicationBeforeAdaptiveActivation) {
  auto config = config_for(salias::Mode::FifoMpsc, "blocking-fixed-unactivated");
  auto created = TypedFifoChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  const Payload expected{.producer = 5, .sequence = 137};

  std::atomic<bool> published{false};
  std::thread delayed_publisher([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    const auto sent = publisher.send(expected);
    published.store(sent.has_value(), std::memory_order_release);
  });
  const auto received = subscriber.recv_copy();
  delayed_publisher.join();

  ASSERT_TRUE(published.load(std::memory_order_acquire));
  ASSERT_TRUE(received);
  EXPECT_EQ(*received, expected);
}

TEST(ChannelApiTest, BlockingRawReceiveWakesForIndependentProcessPublication) {
  auto config = config_for(salias::Mode::FifoMpsc, "blocking-raw-ipc-wake");
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto subscriber = channel.subscriber();

  const auto child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    ::alarm(5);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    auto connected = salias::FifoMpscChannel::connect(config.name);
    if (!connected) {
      ::_exit(1);
    }
    auto publisher = connected->publisher();
    const Payload expected{.producer = 17, .sequence = 231};
    const auto payload = encode(expected);
    const auto offered = publisher.offer(payload);
    ::_exit(offered && *offered ? 0 : 2);
  }

  const auto message = subscriber.recv();
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
  ASSERT_EQ(message.payload.size(), sizeof(Payload));
  EXPECT_EQ(decode(message.payload), (Payload{.producer = 17, .sequence = 231}));
  subscriber.release(message);
}

TEST(ChannelApiTest, SchemaBoundChannelRoundTripsWithFixedClaimSize) {
  auto config = config_for(salias::Mode::FifoMpsc, "schema-fixed-roundtrip");
  auto created = TypedFifoChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  const Payload expected{.producer = 11, .sequence = 42};

  ASSERT_TRUE(publisher.send(expected));
  auto received = subscriber.try_recv_copy();
  ASSERT_TRUE(received);
  ASSERT_TRUE(*received);
  EXPECT_EQ(**received, expected);

  auto claim = publisher.try_claim();
  ASSERT_TRUE(claim);
  auto moved_claim = std::move(*claim);
  EXPECT_EQ(moved_claim.payload().size(), sizeof(Payload));
  EXPECT_TRUE(claim->payload().empty());
  std::memcpy(moved_claim.payload().data(), &expected, sizeof(expected));
  moved_claim.commit();
  EXPECT_TRUE(moved_claim.payload().empty());
  auto claimed = subscriber.recv_copy();
  ASSERT_TRUE(claimed);
  EXPECT_EQ(*claimed, expected);
}

TEST(ChannelApiTest, SchemaIsCheckedOnceWhenConnecting) {
  auto config = config_for(salias::Mode::FifoMpsc, "schema-connect-check");
  auto created = TypedFifoChannel::create(config);
  ASSERT_TRUE(created);
  auto owner = std::move(*created);

  auto matching = TypedFifoChannel::connect(config.name);
  ASSERT_TRUE(matching);
  using DifferentVersion = salias::Channel<salias::Mode::FifoMpsc, PayloadSchemaV2>;
  auto wrong_version = DifferentVersion::connect(config.name);
  ASSERT_FALSE(wrong_version);
  EXPECT_EQ(wrong_version.error(), salias::Error::SchemaMismatch);
  using DifferentId = salias::Channel<salias::Mode::FifoMpsc, OtherPayloadSchema>;
  auto wrong_id = DifferentId::connect(config.name);
  ASSERT_FALSE(wrong_id);
  EXPECT_EQ(wrong_id.error(), salias::Error::SchemaMismatch);
  using DifferentSize = salias::Channel<salias::Mode::FifoMpsc, WiderPayloadSchema>;
  auto wrong_size = DifferentSize::connect(config.name);
  ASSERT_FALSE(wrong_size);
  EXPECT_EQ(wrong_size.error(), salias::Error::SchemaMismatch);
  auto raw = salias::FifoMpscChannel::connect(config.name);
  ASSERT_FALSE(raw);
  EXPECT_EQ(raw.error(), salias::Error::SchemaMismatch);

  auto raw_config = config_for(salias::Mode::FifoMpsc, "schema-rejects-typed");
  auto raw_owner = salias::FifoMpscChannel::create(raw_config);
  ASSERT_TRUE(raw_owner);
  auto typed = TypedFifoChannel::connect(raw_config.name);
  ASSERT_FALSE(typed);
  EXPECT_EQ(typed.error(), salias::Error::SchemaMismatch);
}

template <salias::Mode M>
void check_schema_bound_roundtrip(std::string_view suffix) {
  using Channel = salias::Channel<M, PayloadSchema>;
  auto config = config_for(M, suffix);
  auto created = Channel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  for (std::uint32_t sequence = 0; sequence < 512; ++sequence) {
    const Payload expected{.producer = static_cast<std::uint32_t>(M), .sequence = sequence};
    ASSERT_TRUE(publisher.send(expected));
    auto received = subscriber.try_recv_copy();
    ASSERT_TRUE(received);
    ASSERT_TRUE(*received);
    EXPECT_EQ(**received, expected);
  }
}

TEST(ChannelApiTest, SchemaBoundParserWorksForEveryChannelMode) {
  check_schema_bound_roundtrip<salias::Mode::FifoMpsc>("schema-fifo-mpsc");
  check_schema_bound_roundtrip<salias::Mode::FifoMpmc>("schema-fifo-mpmc");
  check_schema_bound_roundtrip<salias::Mode::OrderedMpsc>("schema-ordered-mpsc");
  check_schema_bound_roundtrip<salias::Mode::OrderedMpmc>("schema-ordered-mpmc");
  check_schema_bound_roundtrip<salias::Mode::LatestMpmc>("schema-latest-mpmc");
}

TEST(ChannelApiTest, SchemaBoundFanoutCopiesIndependently) {
  using Channel = salias::Channel<salias::Mode::FifoMpmc, PayloadSchema>;
  auto config = config_for(salias::Mode::FifoMpmc, "schema-fanout");
  config.num_consumers = 2;
  auto created = Channel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto first = channel.subscriber();
  auto second = channel.subscriber();
  const Payload expected{.producer = 3, .sequence = 19};

  ASSERT_TRUE(publisher.send(expected));
  auto first_copy = first.try_recv_copy();
  auto second_copy = second.try_recv_copy();
  ASSERT_TRUE(first_copy);
  ASSERT_TRUE(second_copy);
  ASSERT_TRUE(*first_copy);
  ASSERT_TRUE(*second_copy);
  EXPECT_EQ(**first_copy, expected);
  EXPECT_EQ(**second_copy, expected);
}

TEST(ChannelApiTest, SchemaBoundLatestReportsLagAndSkipsExpiredFrames) {
  using Channel = salias::Channel<salias::Mode::LatestMpmc, PayloadSchema>;
  auto config = config_for(salias::Mode::LatestMpmc, "schema-latest-lag-ttl");
  config.high_watermark_percent = 50;
  auto created = Channel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  for (std::uint32_t sequence = 0; sequence < 512; ++sequence) {
    ASSERT_TRUE(publisher.send(Payload{.producer = 4, .sequence = sequence}));
  }
  auto lagged = subscriber.try_recv_copy();
  ASSERT_FALSE(lagged);
  EXPECT_EQ(lagged.error(), salias::Error::Lagged);
  auto skipped = subscriber.skip_to_latest();
  ASSERT_TRUE(skipped);
  EXPECT_GT(*skipped, 0u);
  auto latest = subscriber.try_recv_copy();
  ASSERT_TRUE(latest);
  ASSERT_TRUE(*latest);
  EXPECT_EQ((**latest).producer, 4u);
  EXPECT_EQ((**latest).sequence, 511u);

  ASSERT_TRUE(
      publisher.send_for(Payload{.producer = 4, .sequence = 512}, std::chrono::nanoseconds{0}));
  auto expired = subscriber.try_recv_copy();
  ASSERT_TRUE(expired);
  EXPECT_FALSE(*expired);
  EXPECT_EQ(subscriber.expired_messages(), 1u);
}

TEST(ChannelApiTest, SchemaBoundChannelConnectsAcrossForkedProcesses) {
  auto config = config_for(salias::Mode::FifoMpsc, "schema-cross-process");
  auto created = TypedFifoChannel::create(config);
  ASSERT_TRUE(created);
  auto owner = std::move(*created);
  auto subscriber = owner.subscriber();
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    auto connected = TypedFifoChannel::connect(config.name);
    if (!connected) {
      ::_exit(10);
    }
    auto publisher = connected->publisher();
    const auto sent = publisher.send(Payload{.producer = 21, .sequence = 34});
    ::_exit(sent && *sent ? 0 : 11);
  }

  std::optional<Payload> received_payload;
  std::optional<salias::Error> receive_error;
  for (int attempt = 0; attempt < 1'000'000; ++attempt) {
    auto received = subscriber.try_recv_copy();
    if (!received) {
      receive_error = received.error();
      break;
    }
    if (*received) {
      received_payload = **received;
      break;
    }
    std::this_thread::yield();
  }
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
  EXPECT_FALSE(receive_error);
  ASSERT_TRUE(received_payload);
  EXPECT_EQ(*received_payload, (Payload{.producer = 21, .sequence = 34}));
}

TEST(ChannelApiTest, TypedBlockingReceiveRejectsInvalidSubscriber) {
  salias::FifoMpscSubscriber subscriber;
  auto received = subscriber.recv_copy<Payload>();
  ASSERT_FALSE(received);
  EXPECT_EQ(received.error(), salias::Error::BadConfig);
}

TEST(ChannelApiTest, TypedBlockingReceiveContinuesAfterLatestReportsLag) {
  using Channel = salias::LatestMpmcChannel<4096, 50>;
  auto config = config_for(salias::Mode::LatestMpmc, "typed-blocking-lag");
  auto created = Channel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  for (std::uint32_t sequence = 0; sequence < 512; ++sequence) {
    ASSERT_TRUE(publisher.send(Payload{.producer = 1, .sequence = sequence}));
  }

  auto received = subscriber.recv_copy<Payload>();
  ASSERT_TRUE(received);
  EXPECT_EQ(received->producer, 1u);
  EXPECT_GT(received->sequence, 0u);
  EXPECT_LT(received->sequence, 512u);
}

TEST(ChannelApiTest, TypedCopyRejectsWrongPayloadSizeAndContinues) {
  auto config = config_for(salias::Mode::FifoMpsc, "typed-size-mismatch");
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  const std::array<std::byte, sizeof(Payload) + 1> wrong_size{};

  ASSERT_TRUE(publisher.offer(wrong_size));
  auto mismatch = subscriber.try_recv_copy<Payload>();
  ASSERT_FALSE(mismatch);
  EXPECT_EQ(mismatch.error(), salias::Error::PayloadSizeMismatch);

  const Payload expected{.producer = 3, .sequence = 9};
  ASSERT_TRUE(publisher.send(expected));
  auto received = subscriber.try_recv_copy<Payload>();
  ASSERT_TRUE(received);
  ASSERT_TRUE(*received);
  EXPECT_EQ(**received, expected);
}

template <salias::Mode M>
void check_typed_fanout_copy(std::string_view suffix) {
  auto config = config_for(M, suffix);
  config.num_consumers = 2;
  auto created = salias::Channel<M>::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto first = channel.subscriber();
  auto second = channel.subscriber();
  const Payload expected{.producer = 2, .sequence = 17};

  ASSERT_TRUE(publisher.send(expected));
  auto first_copy = first.template try_recv_copy<Payload>();
  auto second_copy = second.template try_recv_copy<Payload>();
  ASSERT_TRUE(first_copy);
  ASSERT_TRUE(second_copy);
  ASSERT_TRUE(*first_copy);
  ASSERT_TRUE(*second_copy);
  EXPECT_EQ(**first_copy, expected);
  EXPECT_EQ(**second_copy, expected);
}

TEST(ChannelApiTest, FifoMpmcTypedCopyIsIndependentPerSubscriber) {
  check_typed_fanout_copy<salias::Mode::FifoMpmc>("typed-fifo-mpmc");
}

TEST(ChannelApiTest, OrderedMpmcTypedCopyIsIndependentPerSubscriber) {
  check_typed_fanout_copy<salias::Mode::OrderedMpmc>("typed-ordered-mpmc");
}

TEST(ChannelApiTest, LatestMpmcTypedCopyIsIndependentPerSubscriber) {
  check_typed_fanout_copy<salias::Mode::LatestMpmc>("typed-latest-mpmc");
}

TEST(ChannelApiTest, FifoThreeProducerScanWrapsAndRemainsFair) {
  auto config = config_for(salias::Mode::FifoMpsc, "three-ring-scan");
  config.num_producers = 3;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  std::array publishers{channel.publisher(), channel.publisher(), channel.publisher()};
  auto subscriber = channel.subscriber();
  ASSERT_TRUE(publishers[2].offer(encode(Payload{.producer = 2})));
  auto last = subscriber.try_recv();
  ASSERT_TRUE(last);
  EXPECT_EQ(last->producer_id, 2u);
  subscriber.release(*last);
  EXPECT_FALSE(subscriber.try_recv());
  for (std::uint32_t sequence = 0; sequence < 3; ++sequence) {
    for (std::uint32_t p = 0; p < 3; ++p) {
      ASSERT_TRUE(publishers[p].offer(encode(Payload{p, sequence})));
    }
  }
  for (std::uint32_t i = 0; i < 9; ++i) {
    auto message = subscriber.try_recv();
    ASSERT_TRUE(message);
    EXPECT_EQ(message->producer_id, i % 3);
    EXPECT_EQ(decode(message->payload).sequence, i / 3);
    subscriber.release(*message);
  }
  EXPECT_FALSE(subscriber.try_recv());
  for (const auto p : {1u, 0u, 2u}) {
    ASSERT_TRUE(publishers[p].offer(encode(Payload{.producer = p})));
    EXPECT_EQ(subscriber.poll(32,
                              [p](const salias::Message& message) noexcept {
                                EXPECT_EQ(message.producer_id, p);
                              }),
              1u);
    EXPECT_FALSE(subscriber.try_recv());
  }
}

template <salias::Mode M>
void check_ordered_batches() {
  auto config = config_for(M, "ordered-cross-ring-batch");
  config.num_producers = 3;
  auto created = salias::Channel<M>::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  std::array publishers{channel.publisher(), channel.publisher(), channel.publisher()};
  auto subscriber = channel.subscriber();
  std::uint64_t expected = 0;
  auto check = [&](const salias::Message& message) noexcept {
    EXPECT_EQ(message.sequence, expected);
    EXPECT_EQ(decode(message.payload).sequence, expected);
    ++expected;
  };
  // More than a ring's capacity over repeated rounds, and more than one chunk
  // per poll. Sequence assignment cycles across a non-power-of-two ring count.
  for (std::uint32_t round = 0; round < 20; ++round) {
    const auto start = static_cast<std::uint32_t>(expected);
    for (std::uint32_t i = 0; i < 99; ++i) {
      const auto p = i % 3;
      ASSERT_TRUE(publishers[p].offer(encode(Payload{p, start + i})));
    }
    EXPECT_EQ(subscriber.poll(1, check), 1u);
    EXPECT_EQ(subscriber.poll(31, check), 31u);
    EXPECT_EQ(subscriber.poll(33, check), 33u);
    EXPECT_EQ(subscriber.poll(100, check), 34u);
    EXPECT_FALSE(subscriber.try_recv());
  }
  auto gap = publishers[2].try_claim(sizeof(Payload));
  ASSERT_TRUE(gap);
  const Payload missing{2, static_cast<std::uint32_t>(expected)};
  std::memcpy(gap->payload().data(), &missing, sizeof(missing));
  ASSERT_TRUE(publishers[0].offer(encode(Payload{0, missing.sequence + 1})));
  EXPECT_EQ(subscriber.poll(32, check), 0u);
  gap->commit();
  EXPECT_EQ(subscriber.poll(32, check), 2u);
  EXPECT_FALSE(subscriber.try_recv());
}

TEST(ChannelApiTest, OrderedMpscBatchesRespectLimitsGapsAndWraparound) {
  check_ordered_batches<salias::Mode::OrderedMpsc>();
}

TEST(ChannelApiTest, OrderedMpmcBatchesRespectLimitsGapsAndWraparound) {
  check_ordered_batches<salias::Mode::OrderedMpmc>();
}

template <salias::Mode M>
void check_batch_retention() {
  auto config = config_for(M, "batch-retention");
  config.num_producers = 3;
  config.num_consumers = 2;
  auto created = salias::Channel<M>::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  std::array publishers{channel.publisher(), channel.publisher(), channel.publisher()};
  auto fast = channel.subscriber();
  auto slow = channel.subscriber();
  for (std::uint32_t p = 0; p < 3; ++p) {
    auto claim = publishers[p].try_claim(4088);
    ASSERT_TRUE(claim);
    std::fill(claim->payload().begin(), claim->payload().end(), static_cast<std::byte>(p));
    claim->commit();
  }
  std::uint32_t callbacks = 0;
  auto check = [&](const salias::Message& message) noexcept {
    if constexpr (M == salias::Mode::OrderedMpmc) {
      EXPECT_EQ(message.sequence, callbacks % 3);
    } else {
      EXPECT_EQ(message.sequence, 0u);
    }
    for (const auto byte : message.payload) {
      EXPECT_EQ(byte, static_cast<std::byte>(message.producer_id));
    }
    EXPECT_FALSE(publishers[message.producer_id].try_claim(4088));
    ++callbacks;
  };
  EXPECT_EQ(fast.poll(32, check), 3u);
  for (auto& publisher : publishers) {
    EXPECT_FALSE(publisher.try_claim(4088));
  }
  // Exercise the function-pointer overload as well as the templated callable.
  EXPECT_EQ(slow.poll(
                32,
                [](const salias::Message& message, void* context) noexcept {
                  (*static_cast<decltype(check)*>(context))(message);
                },
                &check),
            3u);
  EXPECT_EQ(callbacks, 6u);
  for (auto& publisher : publishers) {
    auto next = publisher.try_claim(4088);
    ASSERT_TRUE(next);
    next->commit();
  }
}

TEST(ChannelApiTest, OrderedPollKeepsPayloadUntilCallbacksAndSlowFanoutFinish) {
  check_batch_retention<salias::Mode::OrderedMpmc>();
}

TEST(ChannelApiTest, FifoPollKeepsPayloadUntilCallbacksAndSlowFanoutFinish) {
  check_batch_retention<salias::Mode::FifoMpmc>();
}

template <salias::Mode M>
void check_concurrent_batches() {
  constexpr std::uint32_t kProducers = 3;
  constexpr std::uint32_t kMessages = 20000;
  auto config = config_for(M, "concurrent-batches");
  config.num_producers = kProducers;
  config.num_consumers = 2;
  auto created = salias::Channel<M>::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  std::array publishers{channel.publisher(), channel.publisher(), channel.publisher()};
  std::array subscribers{channel.subscriber(), channel.subscriber()};
  std::atomic<bool> stop{false};
  std::atomic<bool> failed{false};
  std::vector<std::jthread> threads;
  for (std::uint32_t p = 0; p < kProducers; ++p) {
    threads.emplace_back([&, p](std::stop_token token) {
      for (std::uint32_t seq = 0; seq < kMessages;) {
        if (token.stop_requested() || stop.load(std::memory_order_relaxed)) {
          return;
        }
        auto result = publishers[p].offer(encode(Payload{p, seq}));
        if (result && *result) {
          ++seq;
        } else if (!result && result.error() != salias::Error::BackPressured) {
          failed.store(true, std::memory_order_relaxed);
          return;
        } else {
          std::this_thread::yield();
        }
      }
    });
  }
  std::array<std::uint64_t, 2> received{};
  std::array<std::array<std::uint32_t, kProducers>, 2> expected{};
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while ((received[0] != kProducers * kMessages || received[1] != kProducers * kMessages) &&
         !failed.load(std::memory_order_relaxed) && std::chrono::steady_clock::now() < deadline) {
    for (std::size_t c = 0; c < subscribers.size(); ++c) {
      subscribers[c].poll(c == 0 ? 17 : 31, [&, c](const salias::Message& message) noexcept {
        const auto payload = decode(message.payload);
        if constexpr (M == salias::Mode::OrderedMpmc) {
          if (message.sequence != received[c]) {
            failed.store(true, std::memory_order_relaxed);
          }
        } else {
          if (message.sequence != payload.sequence) {
            failed.store(true, std::memory_order_relaxed);
          }
        }
        if (message.producer_id >= kProducers || payload.producer != message.producer_id ||
            payload.sequence != expected[c][message.producer_id]++) {
          failed.store(true, std::memory_order_relaxed);
        }
        ++received[c];
      });
    }
  }
  stop.store(true, std::memory_order_relaxed);
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_FALSE(failed.load(std::memory_order_relaxed));
  for (std::size_t c = 0; c < subscribers.size(); ++c) {
    EXPECT_EQ(received[c], kProducers * kMessages);
    for (const auto count : expected[c]) {
      EXPECT_EQ(count, kMessages);
    }
    EXPECT_FALSE(subscribers[c].try_recv());
  }
}

TEST(ChannelApiTest, OrderedMpmcConcurrentBatchesSurviveBackpressureAndWraparound) {
  check_concurrent_batches<salias::Mode::OrderedMpmc>();
}

TEST(ChannelApiTest, FifoMpmcConcurrentBatchesSurviveBackpressureAndWraparound) {
  check_concurrent_batches<salias::Mode::FifoMpmc>();
}

TEST(ChannelApiTest, FifoBatchesKeepPerRingCursorsAcrossPartialRuns) {
  auto config = config_for(salias::Mode::FifoMpsc, "fifo-partial-runs");
  config.num_producers = 3;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  std::array publishers{channel.publisher(), channel.publisher(), channel.publisher()};
  auto subscriber = channel.subscriber();
  std::array<std::uint32_t, 3> expected{};
  auto check = [&](const salias::Message& message) noexcept {
    ASSERT_LT(message.producer_id, expected.size());
    const auto payload = decode(message.payload);
    EXPECT_EQ(payload.producer, message.producer_id);
    EXPECT_EQ(payload.sequence, expected[message.producer_id]);
    EXPECT_EQ(message.sequence, expected[message.producer_id]++);
  };
  for (std::uint32_t round = 0; round < 20; ++round) {
    for (std::uint32_t p = 0; p < 3; ++p) {
      for (std::uint32_t i = 0; i < 63; ++i) {
        auto result = publishers[p].offer(encode(Payload{p, round * 63 + i}));
        ASSERT_TRUE(result && *result);
      }
    }
    EXPECT_EQ(subscriber.poll(0, check), 0u);
    EXPECT_EQ(subscriber.poll(1, check), 1u);
    EXPECT_EQ(subscriber.poll(31, check), 31u);
    auto single = subscriber.try_recv();
    ASSERT_TRUE(single);
    check(*single);
    subscriber.release(*single);
    EXPECT_EQ(subscriber.poll(33, check), 33u);
    EXPECT_EQ(subscriber.poll(256, check), 123u);
    EXPECT_FALSE(subscriber.try_recv());
  }
  for (const auto sequence : expected) {
    EXPECT_EQ(sequence, 20u * 63);
  }
}

/// @brief 验证 FifoMpsc 下 claim/commit 与 offer 两条发布路径，以及
///   subscriber 持久游标在 release 后能正确推进的往返闭环。
TEST(ChannelApiTest, FifoMpscClaimOfferAndPersistentSubscriberRoundTrip) {
  auto created =
      salias::FifoMpscChannel::create(config_for(salias::Mode::FifoMpsc, "fifo-roundtrip"));
  ASSERT_TRUE(created);
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  // claim 路径：先申请 payload 空间，写入后未 commit 前消费者不可见。
  auto first_claim = publisher.try_claim(sizeof(Payload));
  ASSERT_TRUE(first_claim);
  const Payload first{.producer = 0, .sequence = 1};
  std::memcpy(first_claim->payload().data(), &first, sizeof(first));
  // commit 之前消费者应观测不到该消息。
  EXPECT_FALSE(subscriber.try_recv().has_value());
  first_claim->commit();

  auto first_message = subscriber.try_recv();
  ASSERT_TRUE(first_message);
  EXPECT_EQ(decode(first_message->payload).sequence, 1u);
  // release 释放该消息，推进消费者游标，否则后续消息受流控窗口阻塞。
  subscriber.release(*first_message);

  const auto second = encode(Payload{.producer = 0, .sequence = 2});
  ASSERT_TRUE(publisher.offer(second));
  auto second_message = subscriber.try_recv();
  ASSERT_TRUE(second_message);
  EXPECT_EQ(second_message->sequence, 1u);
  EXPECT_EQ(decode(second_message->payload).sequence, 2u);
  subscriber.release(*second_message);
}

/// @brief 验证 poll 在多次分批调用间保持消费者游标连续，不会重复或遗漏消息。
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

/// @brief 验证 FifoMpsc 模式下消费者按各 producer 的就绪顺序消费，
///   不会因全局序列号缺口而阻塞（与 Ordered 模式的关键区别）。
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
  // 第二个 producer 先发布：FIFO 模式下消费者可立即消费，无需等待第一个。
  ASSERT_TRUE(second_publisher.offer(encode(Payload{.producer = 1, .sequence = 20})));

  auto second_message = subscriber.try_recv();
  ASSERT_TRUE(second_message);
  EXPECT_EQ(second_message->producer_id, 1u);
  subscriber.release(*second_message);

  // 第一个 producer 随后 commit，消费者照样能消费，不受全局序列号约束。
  const Payload first_payload{.producer = 0, .sequence = 10};
  std::memcpy(first->payload().data(), &first_payload, sizeof(first_payload));
  first->commit();
  auto first_message = subscriber.try_recv();
  ASSERT_TRUE(first_message);
  EXPECT_EQ(first_message->producer_id, 0u);
  subscriber.release(*first_message);
}

/// @brief 验证 OrderedMpsc 模式下消费者严格按全局序列号顺序消费：
///   即便后申请的 producer 先 commit，消费者也会等待前序消息就绪后才交付。
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
  // 第二条先 commit，但全局序列号 0 的第一条尚未就绪，消费者必须等待。
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

/// @brief 验证 FifoMpmc 模式下同一条消息会被广播到每一个 subscriber。
TEST(ChannelApiTest, FifoMpmcDeliversEachMessageToEverySubscriber) {
  auto config = config_for(salias::Mode::FifoMpmc, "fifo-fanout");
  config.num_consumers = 2;
  auto created = salias::FifoMpmcChannel::create(config);
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

/// @brief 验证 OrderedMpmc 模式下每个 subscriber 都按全局序列号顺序收到消息，
///   且后序消息在前序消息就绪前对所有 subscriber 不可见。
TEST(ChannelApiTest, OrderedMpmcPreservesGlobalOrderForEverySubscriber) {
  auto config = config_for(salias::Mode::OrderedMpmc, "ordered-fanout");
  config.num_producers = 2;
  config.num_consumers = 2;
  auto created = salias::OrderedMpmcChannel::create(config);
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

/// @brief 验证 offer_batch 一次性发布多条连续消息，消费者可按序逐条接收。
TEST(ChannelApiTest, OfferBatchPublishesContiguousMessages) {
  auto created = salias::FifoMpscChannel::create(config_for(salias::Mode::FifoMpsc, "batch"));
  ASSERT_TRUE(created);
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  const auto first = encode(Payload{.sequence = 1});
  const auto second = encode(Payload{.sequence = 2});
  // 将两条 payload 以 span 数组形式批量提交，减少逐条发布的开销。
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

/// @brief 验证 connect 在模式不匹配时返回 BadConfig——
///   已创建的 FifoMpsc 区域不能用 OrderedMpsc 连接。
TEST(ChannelApiTest, ConnectRejectsDifferentMode) {
  auto config = config_for(salias::Mode::FifoMpsc, "mode-mismatch");
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto connected = salias::OrderedMpscChannel::connect(config.name);
  ASSERT_FALSE(connected);
  EXPECT_EQ(connected.error(), salias::Error::BadConfig);
}

TEST(ChannelApiTest, ExpiredFramesReleaseCapacityWithoutDroppingLiveReliableFrames) {
  auto config = config_for(salias::Mode::FifoMpsc, "ttl-reliable-live-frame");
  config.publication_window = 4096;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array<std::byte, 2000> expired_payload{};
  std::array<std::byte, 2000> live_payload{};
  live_payload.fill(std::byte{0x5a});
  ASSERT_TRUE(publisher.offer_for(expired_payload, std::chrono::nanoseconds{0}));
  ASSERT_TRUE(publisher.offer(live_payload));
  auto blocked = publisher.offer(live_payload);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::Error::BackPressured);

  auto live = subscriber.try_recv();
  ASSERT_TRUE(live);
  EXPECT_EQ(live->payload.size(), live_payload.size());
  EXPECT_EQ(live->expires_at_monotonic_ns, 0u);
  EXPECT_TRUE(std::equal(live->payload.begin(), live->payload.end(), live_payload.begin()));
  EXPECT_EQ(subscriber.expired_messages(), 1u);
  ASSERT_TRUE(publisher.offer(live_payload));
  blocked = publisher.offer(live_payload);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::Error::BackPressured);

  subscriber.release(*live);
  ASSERT_TRUE(publisher.offer(live_payload));
  live = subscriber.try_recv();
  ASSERT_TRUE(live);
  EXPECT_TRUE(std::equal(live->payload.begin(), live->payload.end(), live_payload.begin()));
  subscriber.release(*live);
  live = subscriber.try_recv();
  ASSERT_TRUE(live);
  EXPECT_TRUE(std::equal(live->payload.begin(), live->payload.end(), live_payload.begin()));
  subscriber.release(*live);
}

TEST(ChannelApiTest, PollReleasesExpiredPrefixBeforeBorrowedLiveFrame) {
  auto config = config_for(salias::Mode::FifoMpsc, "ttl-poll-prefix-capacity");
  config.publication_window = 4096;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array<std::byte, 2000> payload{};
  payload.fill(std::byte{0x71});
  ASSERT_TRUE(publisher.offer_for(payload, std::chrono::nanoseconds{0}));
  ASSERT_TRUE(publisher.offer(payload));
  auto full = publisher.offer(payload);
  ASSERT_FALSE(full);
  EXPECT_EQ(full.error(), salias::Error::BackPressured);

  bool reclaimed_prefix = false;
  bool live_remained_backpressured = false;
  const auto polled = subscriber.poll(1, [&](const salias::Message& message) noexcept {
    EXPECT_TRUE(std::equal(message.payload.begin(), message.payload.end(), payload.begin()));
    const auto next = publisher.offer(payload);
    reclaimed_prefix = next && *next;
    const auto still_full = publisher.offer(payload);
    live_remained_backpressured = !still_full && still_full.error() == salias::Error::BackPressured;
  });
  EXPECT_EQ(polled, 1u);
  EXPECT_TRUE(reclaimed_prefix);
  EXPECT_TRUE(live_remained_backpressured);
  EXPECT_EQ(subscriber.expired_messages(), 1u);

  ASSERT_TRUE(publisher.offer(payload));
  EXPECT_EQ(subscriber.poll(3, [&](const salias::Message& message) noexcept {
              EXPECT_TRUE(std::equal(message.payload.begin(), message.payload.end(), payload.begin()));
            }),
            2u);
}

TEST(ChannelApiTest, BorrowedTtlMessageRemainsPinnedAfterItsDeadline) {
  auto config = config_for(salias::Mode::FifoMpsc, "ttl-borrowed-deadline");
  config.publication_window = 4096;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array<std::byte, 2000> payload{};
  payload.fill(std::byte{0x4d});
  ASSERT_TRUE(publisher.offer_for(payload, std::chrono::milliseconds{500}));
  auto borrowed = subscriber.try_recv();
  ASSERT_TRUE(borrowed);
  ASSERT_TRUE(publisher.offer(payload));
  std::this_thread::sleep_for(std::chrono::milliseconds{510});

  EXPECT_LE(borrowed->expires_at_monotonic_ns,
            salias::platform::monotonic_now_ns().value_or(0));
  EXPECT_TRUE(std::equal(borrowed->payload.begin(), borrowed->payload.end(), payload.begin()));
  auto blocked = publisher.offer(payload);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::Error::BackPressured);

  subscriber.release(*borrowed);
  auto next = subscriber.try_recv();
  ASSERT_TRUE(next);
  subscriber.release(*next);
  ASSERT_TRUE(publisher.offer(payload));
}

TEST(ChannelApiTest, TtlClaimBatchPartialPrefixAndWraparoundAreHandled) {
  auto config = config_for(salias::Mode::FifoMpsc, "ttl-batch-wraparound");
  config.publication_window = 4096;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array<std::byte, 1024> payload{};
  std::array<std::span<const std::byte>, 4> batch{payload, payload, payload, payload};
  auto published = publisher.offer_batch_for(batch, std::chrono::nanoseconds{0});
  ASSERT_TRUE(published);
  EXPECT_EQ(*published, 3u);
  EXPECT_EQ(subscriber.poll(4, [](const salias::Message&) noexcept {}), 0u);
  EXPECT_EQ(subscriber.expired_messages(), 3u);

  for (std::uint64_t sequence = 0; sequence < 12; ++sequence) {
    auto claim = publisher.try_claim_for(sizeof(Payload), std::chrono::seconds{1});
    ASSERT_TRUE(claim);
    const auto encoded = encode(Payload{.sequence = static_cast<std::uint32_t>(sequence)});
    std::memcpy(claim->payload().data(), encoded.data(), encoded.size());
    claim->commit();
    auto message = subscriber.try_recv();
    ASSERT_TRUE(message);
    EXPECT_GT(message->expires_at_monotonic_ns, 0u);
    EXPECT_EQ(decode(message->payload).sequence, sequence);
    subscriber.release(*message);
  }

  std::array<std::byte, 2000> variable_a{};
  std::array<std::byte, 2000> variable_b{};
  std::array<std::byte, 2000> variable_c{};
  const std::array<std::span<const std::byte>, 3> variable_batch{variable_a, variable_b,
                                                                 variable_c};
  auto variable_published = publisher.offer_batch_for(variable_batch, std::chrono::seconds{1});
  ASSERT_TRUE(variable_published);
  EXPECT_EQ(*variable_published, 2u);
  std::uint64_t shared_deadline = 0;
  bool deadlines_match = true;
  const auto variable_polled = subscriber.poll(3, [&](const salias::Message& message) noexcept {
    if (shared_deadline == 0) {
      shared_deadline = message.expires_at_monotonic_ns;
    } else {
      deadlines_match = deadlines_match && message.expires_at_monotonic_ns == shared_deadline;
    }
  });
  EXPECT_EQ(variable_polled, 2u);
  EXPECT_TRUE(deadlines_match);
  EXPECT_GT(shared_deadline, 0u);

  std::array<std::byte, 2040> uniform_payload{};
  const std::array<std::span<const std::byte>, 3> uniform_batch{
      uniform_payload, uniform_payload, uniform_payload};
  const auto uniform_published = publisher.offer_batch_for(uniform_batch, std::chrono::seconds{1});
  ASSERT_TRUE(uniform_published);
  EXPECT_EQ(*uniform_published, 1u);
  std::uint64_t uniform_deadline = 0;
  bool uniform_deadlines_match = true;
  const auto uniform_polled = subscriber.poll(3, [&](const salias::Message& message) noexcept {
    if (uniform_deadline == 0) {
      uniform_deadline = message.expires_at_monotonic_ns;
    } else {
      uniform_deadlines_match =
          uniform_deadlines_match && message.expires_at_monotonic_ns == uniform_deadline;
    }
  });
  EXPECT_EQ(uniform_polled, 1u);
  EXPECT_TRUE(uniform_deadlines_match);
  EXPECT_GT(uniform_deadline, 0u);

  const auto negative = publisher.offer_for(payload, std::chrono::nanoseconds{-1});
  ASSERT_FALSE(negative);
  EXPECT_EQ(negative.error(), salias::Error::BadConfig);
  const auto negative_claim = publisher.try_claim_for(8, std::chrono::nanoseconds{-1});
  ASSERT_FALSE(negative_claim);
  EXPECT_EQ(negative_claim.error(), salias::Error::BadConfig);
  const auto negative_batch = publisher.offer_batch_for(batch, std::chrono::nanoseconds{-1});
  ASSERT_FALSE(negative_batch);
  EXPECT_EQ(negative_batch.error(), salias::Error::BadConfig);
}

TEST(ChannelApiTest, OrderedTtlSkipPreservesGlobalSequenceAcrossProducerRings) {
  auto config = config_for(salias::Mode::OrderedMpsc, "ttl-ordered-rings");
  config.num_producers = 2;
  auto created = salias::OrderedMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto first_publisher = channel.publisher();
  auto second_publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  ASSERT_TRUE(first_publisher.offer_for(encode(Payload{0, 0}), std::chrono::nanoseconds{0}));
  ASSERT_TRUE(second_publisher.offer(encode(Payload{1, 1})));
  auto message = subscriber.try_recv();
  ASSERT_TRUE(message);
  EXPECT_EQ(message->sequence, 1u);
  EXPECT_EQ(decode(message->payload), (Payload{1, 1}));
  EXPECT_EQ(subscriber.expired_messages(), 1u);
  subscriber.release(*message);
}

TEST(ChannelApiTest, LatestMpmcTtlFramesAreParsedDuringRetentionAndLagRecovery) {
  auto config = config_for(salias::Mode::LatestMpmc, "ttl-latest-retention");
  config.publication_window_messages = 2;
  auto created = salias::LatestMpmcChannel<4096>::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  ASSERT_TRUE(publisher.offer_for(encode(Payload{0, 0}), std::chrono::seconds{1}));
  ASSERT_TRUE(publisher.offer_for(encode(Payload{0, 1}), std::chrono::nanoseconds{0}));
  ASSERT_TRUE(publisher.offer(encode(Payload{0, 2})));

  auto lagged = subscriber.try_recv_checked();
  ASSERT_FALSE(lagged);
  EXPECT_EQ(lagged.error(), salias::Error::Lagged);
  EXPECT_EQ(subscriber.lagged_messages(), 1u);

  auto latest = subscriber.try_recv();
  ASSERT_TRUE(latest);
  EXPECT_EQ(decode(latest->payload), (Payload{0, 2}));
  EXPECT_EQ(subscriber.expired_messages(), 1u);
  subscriber.release(*latest);

  const auto expired_a = encode(Payload{0, 3});
  const auto expired_b = encode(Payload{0, 4});
  const auto expired_c = encode(Payload{0, 5});
  const std::array<std::span<const std::byte>, 3> expired_batch{expired_a, expired_b, expired_c};
  const auto batch_published =
      publisher.offer_batch_for(expired_batch, std::chrono::nanoseconds{0});
  ASSERT_TRUE(batch_published);
  EXPECT_EQ(*batch_published, 2u);
  EXPECT_EQ(subscriber.poll(3, [](const salias::Message&) noexcept {}), 0u);
  EXPECT_EQ(subscriber.expired_messages(), 3u);
}

TEST(ChannelApiTest, TtlDeadlineUsesSharedMonotonicClockAcrossProcesses) {
  auto config = config_for(salias::Mode::FifoMpsc, "ttl-cross-process-clock");
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto owner = std::move(*created);

  int ready_pipe[2]{};
  ASSERT_EQ(::pipe(ready_pipe), 0);
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(ready_pipe[0]));
    auto connected = salias::FifoMpscChannel::connect(config.name);
    if (!connected) {
      ::_exit(10);
    }
    auto subscriber = connected->subscriber();
    const char ready = 'r';
    if (::write(ready_pipe[1], &ready, sizeof(ready)) != sizeof(ready)) {
      ::_exit(11);
    }
    for (int attempt = 0; attempt < 1'000'000; ++attempt) {
      if (auto message = subscriber.try_recv()) {
        const bool valid =
            decode(message->payload) == Payload{7, 1} && subscriber.expired_messages() == 1;
        subscriber.release(*message);
        ::_exit(valid ? 0 : 12);
      }
      std::this_thread::yield();
    }
    ::_exit(13);
  }

  static_cast<void>(::close(ready_pipe[1]));
  char ready = 0;
  ASSERT_EQ(::read(ready_pipe[0], &ready, sizeof(ready)), sizeof(ready));
  ASSERT_EQ(ready, 'r');
  auto publisher = owner.publisher();
  ASSERT_TRUE(publisher.offer_for(encode(Payload{7, 0}), std::chrono::nanoseconds{0}));
  ASSERT_TRUE(publisher.offer(encode(Payload{7, 1})));
  static_cast<void>(::close(ready_pipe[0]));

  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(ChannelApiTest, MessageCountWindowLimitsEachOrderedProducerAndResumesAfterRelease) {
  auto config = config_for(salias::Mode::OrderedMpsc, "message-window-ordered-producers");
  config.num_producers = 2;
  config.publication_window_messages = 1;
  auto created = salias::OrderedMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto first_publisher = channel.publisher();
  auto second_publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  const auto first = encode(Payload{0, 0});
  const auto blocked = encode(Payload{0, 1});
  const auto second_ring = encode(Payload{1, 0});
  const auto resumed = encode(Payload{0, 2});
  ASSERT_TRUE(first_publisher.offer(first));
  auto full = first_publisher.offer(blocked);
  ASSERT_FALSE(full);
  EXPECT_EQ(full.error(), salias::Error::BackPressured);
  ASSERT_TRUE(second_publisher.offer(second_ring));

  auto message = subscriber.try_recv();
  ASSERT_TRUE(message);
  EXPECT_EQ(message->sequence, 0u);
  const auto decoded_first = decode(message->payload);
  EXPECT_EQ(decoded_first.producer, 0u);
  EXPECT_EQ(decoded_first.sequence, 0u);
  subscriber.release(*message);
  ASSERT_TRUE(first_publisher.offer(resumed));

  message = subscriber.try_recv();
  ASSERT_TRUE(message);
  EXPECT_EQ(message->sequence, 1u);
  EXPECT_EQ(decode(message->payload), (Payload{1, 0}));
  subscriber.release(*message);
  message = subscriber.try_recv();
  ASSERT_TRUE(message);
  EXPECT_EQ(message->sequence, 2u);
  EXPECT_EQ(decode(message->payload), (Payload{0, 2}));
  subscriber.release(*message);
}

TEST(ChannelApiTest, ByteAndMessageWindowsApplyIndependently) {
  auto config = config_for(salias::Mode::FifoMpsc, "byte-and-message-windows");
  config.capacity = 8192;
  config.publication_window = 4096;
  config.publication_window_messages = 3;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array<std::byte, 2040> large_payload{};
  ASSERT_TRUE(publisher.offer(large_payload));
  ASSERT_TRUE(publisher.offer(large_payload));
  auto byte_limited = publisher.offer(large_payload);
  ASSERT_FALSE(byte_limited);
  EXPECT_EQ(byte_limited.error(), salias::Error::BackPressured);

  auto first_large = subscriber.try_recv();
  ASSERT_TRUE(first_large);
  EXPECT_EQ(first_large->payload.size(), large_payload.size());
  subscriber.release(*first_large);
  ASSERT_TRUE(publisher.offer(large_payload));
  for (std::uint64_t sequence = 1; sequence <= 2; ++sequence) {
    auto message = subscriber.try_recv();
    ASSERT_TRUE(message);
    EXPECT_EQ(message->sequence, sequence);
    subscriber.release(*message);
  }

  const auto small_payload = encode(Payload{0, 0});
  for (int index = 0; index < 3; ++index) {
    ASSERT_TRUE(publisher.offer(small_payload));
  }
  auto message_limited = publisher.offer(small_payload);
  ASSERT_FALSE(message_limited);
  EXPECT_EQ(message_limited.error(), salias::Error::BackPressured);

  auto first_small = subscriber.try_recv();
  ASSERT_TRUE(first_small);
  subscriber.release(*first_small);
  ASSERT_TRUE(publisher.offer(small_payload));
}

TEST(ChannelApiTest, MessageCountWindowConstrainsBatchesAndPersistsAcrossConnect) {
  auto config = config_for(salias::Mode::FifoMpsc, "message-window-batch-connect");
  config.publication_window_messages = 3;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);
  auto owner = std::move(*created);
  auto connected = salias::FifoMpscChannel::connect(config.name);
  ASSERT_TRUE(connected);
  auto publisher = owner.publisher();
  auto subscriber = connected->subscriber();

  const std::array<Payload, 5> values = {Payload{0, 0}, Payload{0, 1}, Payload{0, 2}, Payload{0, 3},
                                         Payload{0, 4}};
  std::array<std::array<std::byte, sizeof(Payload)>, values.size()> encoded{};
  std::array<std::span<const std::byte>, values.size()> payloads{};
  for (std::size_t index = 0; index < values.size(); ++index) {
    encoded[index] = encode(values[index]);
    payloads[index] = encoded[index];
  }

  auto published = publisher.offer_batch(payloads);
  ASSERT_TRUE(published);
  EXPECT_EQ(*published, 3u);
  auto blocked = publisher.offer(payloads[3]);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::Error::BackPressured);

  Payload received_first{};
  EXPECT_EQ(subscriber.poll(1,
                            [&](const salias::Message& message) noexcept {
                              received_first = decode(message.payload);
                            }),
            1u);
  EXPECT_EQ(received_first, values[0]);
  ASSERT_TRUE(publisher.offer(payloads[3]));

  for (std::size_t index = 1; index <= 3; ++index) {
    auto message = subscriber.try_recv();
    ASSERT_TRUE(message);
    EXPECT_EQ(decode(message->payload), values[index]);
    subscriber.release(*message);
  }
}

TEST(ChannelApiTest, MessageCountWindowWaitsForSlowestFanoutConsumer) {
  auto config = config_for(salias::Mode::FifoMpmc, "message-window-fanout-minimum");
  config.num_consumers = 2;
  config.publication_window_messages = 2;
  auto created = salias::FifoMpmcChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto fast = channel.subscriber();
  auto slow = channel.subscriber();

  const std::array payloads = {encode(Payload{0, 0}), encode(Payload{0, 1}), encode(Payload{0, 2})};
  ASSERT_TRUE(publisher.offer(payloads[0]));
  ASSERT_TRUE(publisher.offer(payloads[1]));
  for (std::size_t index = 0; index < 2; ++index) {
    auto message = fast.try_recv();
    ASSERT_TRUE(message);
    EXPECT_EQ(decode(message->payload), (Payload{0, static_cast<std::uint32_t>(index)}));
    fast.release(*message);
  }

  auto blocked = publisher.offer(payloads[2]);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::Error::BackPressured);

  auto first = slow.try_recv();
  ASSERT_TRUE(first);
  EXPECT_EQ(decode(first->payload), (Payload{0, 0}));
  slow.release(*first);
  ASSERT_TRUE(publisher.offer(payloads[2]));

  auto fast_latest = fast.try_recv();
  ASSERT_TRUE(fast_latest);
  EXPECT_EQ(decode(fast_latest->payload), (Payload{0, 2}));
  fast.release(*fast_latest);
  for (std::uint32_t sequence = 1; sequence <= 2; ++sequence) {
    auto message = slow.try_recv();
    ASSERT_TRUE(message);
    EXPECT_EQ(decode(message->payload), (Payload{0, sequence}));
    slow.release(*message);
  }
}

TEST(ChannelApiTest, LatestMpmcUsesStaticCapacityAndWatermark) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto config = config_for(salias::Mode::LatestMpmc, "latest-static-config");
  config.capacity = 8192;
  config.high_watermark_percent = 90;

  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  EXPECT_EQ(TypedChannel::capacity(), 4096u);
  EXPECT_EQ(TypedChannel::high_watermark_percent(), 50u);
  EXPECT_EQ(created->capacity(), 4096u);
  EXPECT_EQ(created->high_watermark_percent(), 50u);

  auto connected = TypedChannel::connect(config.name);
  ASSERT_TRUE(connected);
  EXPECT_EQ(connected->capacity(), 4096u);
  EXPECT_EQ(connected->high_watermark_percent(), 50u);

  auto wrong_capacity = salias::LatestMpmcChannel<8192, 50>::connect(config.name);
  ASSERT_FALSE(wrong_capacity);
  EXPECT_EQ(wrong_capacity.error(), salias::Error::BadConfig);

  auto wrong_watermark = salias::LatestMpmcChannel<4096, 90>::connect(config.name);
  ASSERT_FALSE(wrong_watermark);
  EXPECT_EQ(wrong_watermark.error(), salias::Error::BadConfig);
}

TEST(ChannelApiTest, LatestMpmcSupportsDefaultMinimumAndFullWatermarks) {
  using DefaultWatermark = salias::LatestMpmcChannel<4096>;
  using MinimumWatermark = salias::LatestMpmcChannel<4096, 1>;
  using FullWatermark = salias::LatestMpmcChannel<4096, 100>;
  static_assert(DefaultWatermark::high_watermark_percent() == 90);
  static_assert(MinimumWatermark::high_watermark_percent() == 1);
  static_assert(FullWatermark::high_watermark_percent() == 100);

  auto default_created =
      DefaultWatermark::create(config_for(salias::Mode::LatestMpmc, "latest-default-watermark"));
  ASSERT_TRUE(default_created);
  EXPECT_EQ(default_created->high_watermark_percent(), 90u);

  auto minimum_created =
      MinimumWatermark::create(config_for(salias::Mode::LatestMpmc, "latest-minimum-watermark"));
  ASSERT_TRUE(minimum_created);
  EXPECT_EQ(minimum_created->high_watermark_percent(), 1u);

  auto full_created =
      FullWatermark::create(config_for(salias::Mode::LatestMpmc, "latest-full-watermark"));
  ASSERT_TRUE(full_created);
  EXPECT_EQ(full_created->high_watermark_percent(), 100u);
}

TEST(ChannelApiTest, LatestMpmcRejectsIncompatibleFlowControlConfig) {
  auto invalid_watermark = config_for(salias::Mode::LatestMpmc, "latest-invalid-watermark");
  invalid_watermark.high_watermark_percent = 101;
  auto bad_watermark = salias::Channel<salias::Mode::LatestMpmc>::create(invalid_watermark);
  ASSERT_FALSE(bad_watermark);
  EXPECT_EQ(bad_watermark.error(), salias::Error::BadConfig);

  auto zero_watermark = config_for(salias::Mode::LatestMpmc, "latest-zero-watermark");
  zero_watermark.high_watermark_percent = 0;
  auto bad_zero_watermark = salias::Channel<salias::Mode::LatestMpmc>::create(zero_watermark);
  ASSERT_FALSE(bad_zero_watermark);
  EXPECT_EQ(bad_zero_watermark.error(), salias::Error::BadConfig);

  auto publication_window = config_for(salias::Mode::LatestMpmc, "latest-publication-window");
  publication_window.publication_window = publication_window.capacity;
  auto bad_window = salias::Channel<salias::Mode::LatestMpmc>::create(publication_window);
  ASSERT_FALSE(bad_window);
  EXPECT_EQ(bad_window.error(), salias::Error::BadConfig);
}

TEST(ChannelApiTest, LatestMpmcMessageWindowRetainsLatestFramesAndReliableReadersBackpressure) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 100>;
  auto config = config_for(salias::Mode::LatestMpmc, "latest-message-window-drop-oldest");
  config.publication_window_messages = 3;
  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  for (std::uint64_t sequence = 0; sequence < 6; ++sequence) {
    const auto payload = encode(Payload{0, static_cast<std::uint32_t>(sequence)});
    ASSERT_TRUE(publisher.offer(payload));
  }

  auto lagged = subscriber.try_recv_checked();
  ASSERT_FALSE(lagged);
  EXPECT_EQ(lagged.error(), salias::Error::Lagged);
  EXPECT_EQ(subscriber.lagged_messages(), 3u);
  for (std::uint64_t sequence = 3; sequence < 6; ++sequence) {
    auto message = subscriber.try_recv_checked();
    ASSERT_TRUE(message);
    ASSERT_TRUE(*message);
    EXPECT_EQ(decode((**message).payload).sequence, sequence);
    subscriber.release(**message);
  }

  auto reliable_config =
      config_for(salias::Mode::LatestMpmc, "latest-message-window-reliable-reader");
  reliable_config.publication_window_messages = 3;
  reliable_config.num_consumers = 2;
  auto reliable_created = TypedChannel::create(reliable_config);
  ASSERT_TRUE(reliable_created);
  auto reliable_channel = std::move(*reliable_created);
  auto reliable_publisher = reliable_channel.publisher();
  auto reliable = reliable_channel.subscriber(salias::SubscriptionPolicy::Reliable);
  auto lossy = reliable_channel.subscriber(salias::SubscriptionPolicy::DropOldest);
  for (std::uint64_t sequence = 0; sequence < 3; ++sequence) {
    const auto payload = encode(Payload{0, static_cast<std::uint32_t>(sequence)});
    ASSERT_TRUE(reliable_publisher.offer(payload));
  }
  const auto boundary = encode(Payload{0, 3});
  const auto remainder = encode(Payload{0, 4});
  const std::array<std::span<const std::byte>, 2> batch_payloads = {boundary, remainder};
  auto blocked = reliable_publisher.offer(boundary);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::Error::BackPressured);

  auto first = reliable.try_recv_checked();
  ASSERT_TRUE(first);
  ASSERT_TRUE(*first);
  EXPECT_EQ(decode((**first).payload).sequence, 0u);
  reliable.release(**first);
  auto partial = reliable_publisher.offer_batch(batch_payloads);
  ASSERT_TRUE(partial);
  EXPECT_EQ(*partial, 1u);
  auto lossy_lag = lossy.try_recv_checked();
  ASSERT_FALSE(lossy_lag);
  EXPECT_EQ(lossy_lag.error(), salias::Error::Lagged);
  EXPECT_EQ(lossy.lagged_messages(), 1u);

  auto next_reliable = reliable.try_recv_checked();
  ASSERT_TRUE(next_reliable);
  ASSERT_TRUE(*next_reliable);
  EXPECT_EQ(decode((**next_reliable).payload).sequence, 1u);
  reliable.release(**next_reliable);
  ASSERT_TRUE(reliable_publisher.offer(batch_payloads[1]));
  for (std::uint32_t sequence = 2; sequence <= 4; ++sequence) {
    auto message = reliable.try_recv_checked();
    ASSERT_TRUE(message);
    ASSERT_TRUE(*message);
    EXPECT_EQ(decode((**message).payload).sequence, sequence);
    reliable.release(**message);
  }
}

TEST(ChannelApiTest, LatestMpmcMessageWindowConstrainsBatchesAndSurvivesVariableLengthWraparound) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 100>;
  auto config = config_for(salias::Mode::LatestMpmc, "latest-message-window-batch-wrap");
  config.publication_window_messages = 3;
  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  const std::array<Payload, 5> batch_values = {Payload{0, 0}, Payload{0, 1}, Payload{0, 2},
                                               Payload{0, 3}, Payload{0, 4}};
  std::array<std::array<std::byte, sizeof(Payload)>, batch_values.size()> encoded_batch{};
  std::array<std::span<const std::byte>, batch_values.size()> batch_payloads{};
  for (std::size_t index = 0; index < batch_values.size(); ++index) {
    encoded_batch[index] = encode(batch_values[index]);
    batch_payloads[index] = encoded_batch[index];
  }
  auto batch = publisher.offer_batch(batch_payloads);
  ASSERT_TRUE(batch);
  EXPECT_EQ(*batch, 3u);
  ASSERT_TRUE(publisher.offer(batch_payloads[3]));
  ASSERT_TRUE(publisher.offer(batch_payloads[4]));
  auto lagged = subscriber.try_recv_checked();
  ASSERT_FALSE(lagged);
  EXPECT_EQ(lagged.error(), salias::Error::Lagged);
  EXPECT_EQ(subscriber.lagged_messages(), 2u);
  for (std::uint32_t sequence = 2; sequence <= 4; ++sequence) {
    auto message = subscriber.try_recv_checked();
    ASSERT_TRUE(message);
    ASSERT_TRUE(*message);
    EXPECT_EQ(decode((**message).payload).sequence, sequence);
    subscriber.release(**message);
  }

  constexpr std::array<std::size_t, 7> lengths = {0, 1, 17, 129, 257, 509, 997};
  constexpr std::uint64_t total = 300;
  const auto previous_lagged = subscriber.lagged_messages();
  std::array<std::byte, lengths.back()> payload{};
  for (std::uint64_t sequence = 0; sequence < total; ++sequence) {
    const auto length = lengths[sequence % lengths.size()];
    std::fill_n(payload.begin(), length, static_cast<std::byte>(sequence & 0xffu));
    ASSERT_TRUE(publisher.offer(std::span<const std::byte>{payload.data(), length}));
  }
  lagged = subscriber.try_recv_checked();
  ASSERT_FALSE(lagged);
  EXPECT_EQ(lagged.error(), salias::Error::Lagged);
  EXPECT_EQ(subscriber.lagged_messages(), previous_lagged + total - 3);
  for (std::uint64_t sequence = total - 3; sequence < total; ++sequence) {
    auto message = subscriber.try_recv_checked();
    ASSERT_TRUE(message);
    ASSERT_TRUE(*message);
    EXPECT_EQ((**message).sequence, sequence + 5);
    const auto expected_length = lengths[sequence % lengths.size()];
    EXPECT_EQ((**message).payload.size(), expected_length);
    EXPECT_TRUE(std::all_of(
        (**message).payload.begin(), (**message).payload.end(),
        [sequence](std::byte value) { return value == static_cast<std::byte>(sequence & 0xffu); }));
    subscriber.release(**message);
  }
}

TEST(ChannelApiTest, LatestMpmcDropsOldMessagesAndPinsBorrowedPayload) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto config = config_for(salias::Mode::LatestMpmc, "latest-drop-oldest");
  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 0})));
  auto borrowed = subscriber.try_recv();
  ASSERT_TRUE(borrowed);

  for (std::uint32_t sequence = 1; sequence < 256; ++sequence) {
    ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = sequence}))) << sequence;
  }
  EXPECT_EQ(decode(borrowed->payload).sequence, 0u);

  auto pinned_write = publisher.offer(encode(Payload{.sequence = 256}));
  ASSERT_FALSE(pinned_write);
  EXPECT_EQ(pinned_write.error(), salias::Error::BackPressured);
  EXPECT_EQ(decode(borrowed->payload).sequence, 0u);

  const auto next_payload = encode(Payload{.sequence = 256});
  const std::array<std::span<const std::byte>, 2> next_batch{next_payload, next_payload};
  auto pinned_batch = publisher.offer_batch(next_batch);
  ASSERT_FALSE(pinned_batch);
  EXPECT_EQ(pinned_batch.error(), salias::Error::BackPressured);

  subscriber.release(*borrowed);
  auto oldest_after_failed_write = subscriber.try_recv();
  ASSERT_TRUE(oldest_after_failed_write);
  EXPECT_EQ(decode(oldest_after_failed_write->payload).sequence, 128u);
  subscriber.release(*oldest_after_failed_write);

  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 256})));

  auto latest = subscriber.try_recv();
  ASSERT_TRUE(latest);
  EXPECT_EQ(decode(latest->payload).sequence, 129u);
  subscriber.release(*latest);
}

TEST(ChannelApiTest, LatestMpmcHandlesEmptyAndOverWatermarkFrames) {
  using TinyWindow = salias::LatestMpmcChannel<4096, 1>;
  auto created =
      TinyWindow::create(config_for(salias::Mode::LatestMpmc, "latest-small-window-frames"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  const std::span<const std::byte> empty_payload;
  ASSERT_TRUE(publisher.offer(empty_payload));
  auto empty_message = subscriber.try_recv();
  ASSERT_TRUE(empty_message);
  EXPECT_TRUE(empty_message->payload.empty());
  subscriber.release(*empty_message);

  // 1% of 4096 is 40 bytes: a 32-byte payload fits, 33 bytes rounds up to 48.
  std::array<std::byte, 33> too_large_for_window{};
  auto rejected = publisher.offer(too_large_for_window);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), salias::Error::MessageTooLarge);
  auto rejected_claim = publisher.try_claim(too_large_for_window.size());
  ASSERT_FALSE(rejected_claim);
  EXPECT_EQ(rejected_claim.error(), salias::Error::MessageTooLarge);
  auto huge_claim = publisher.try_claim(std::numeric_limits<std::size_t>::max());
  ASSERT_FALSE(huge_claim);
  EXPECT_EQ(huge_claim.error(), salias::Error::MessageTooLarge);

  std::array<std::byte, 32> exact_fit{};
  ASSERT_TRUE(publisher.offer(exact_fit));
  auto largest = subscriber.try_recv();
  ASSERT_TRUE(largest);
  EXPECT_EQ(largest->payload.size(), exact_fit.size());
  EXPECT_EQ(largest->sequence, 1u);
  subscriber.release(*largest);

  auto zero_frame = publisher.try_claim(0);
  ASSERT_TRUE(zero_frame);
  zero_frame->commit();
  auto claimed_empty_message = subscriber.try_recv();
  ASSERT_TRUE(claimed_empty_message);
  EXPECT_TRUE(claimed_empty_message->payload.empty());
  subscriber.release(*claimed_empty_message);
}

TEST(ChannelApiTest, LatestMpmcFullWatermarkSupportsMaximumFramesAndPinnedRetry) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 100>;
  auto created =
      TypedChannel::create(config_for(salias::Mode::LatestMpmc, "latest-full-ring-frame"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  std::array<std::byte, 4088> payload{};
  payload.fill(std::byte{0xa5});
  ASSERT_TRUE(publisher.offer(payload));
  auto held = subscriber.try_recv();
  ASSERT_TRUE(held);
  auto blocked = publisher.offer(payload);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::Error::BackPressured);
  EXPECT_TRUE(std::ranges::equal(held->payload, payload));
  subscriber.release(*held);

  auto claim = publisher.try_claim(payload.size());
  ASSERT_TRUE(claim);
  std::memcpy(claim->payload().data(), payload.data(), payload.size());
  EXPECT_FALSE(subscriber.try_recv());
  claim->commit();
  auto message = subscriber.try_recv();
  ASSERT_TRUE(message);
  EXPECT_EQ(message->sequence, 1u);
  EXPECT_TRUE(std::ranges::equal(message->payload, payload));
  subscriber.release(*message);
}

TEST(ChannelApiTest, LatestMpmcEmptyAndMixedBatchesReportPartialProgress) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 1>;
  auto created =
      TypedChannel::create(config_for(salias::Mode::LatestMpmc, "latest-partial-batches"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  auto no_messages = publisher.offer_batch({});
  ASSERT_TRUE(no_messages);
  EXPECT_EQ(*no_messages, 0u);
  const std::array<std::span<const std::byte>, 6> empty_messages{};
  auto empty_batch = publisher.offer_batch(empty_messages);
  ASSERT_TRUE(empty_batch);
  EXPECT_EQ(*empty_batch, 5u);
  std::uint64_t sequence = 0;
  EXPECT_EQ(subscriber.poll(10,
                            [&sequence](const salias::Message& message) noexcept {
                              EXPECT_TRUE(message.payload.empty());
                              EXPECT_EQ(message.sequence, sequence++);
                            }),
            5u);

  const auto valid = encode(Payload{.sequence = 5});
  const std::array<std::byte, 33> oversized{};
  const std::array<std::span<const std::byte>, 2> invalid_uniform{oversized, oversized};
  auto rejected = publisher.offer_batch(invalid_uniform);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), salias::Error::MessageTooLarge);
  const std::array<std::span<const std::byte>, 2> mixed{valid, oversized};
  auto partial = publisher.offer_batch(mixed);
  ASSERT_TRUE(partial);
  EXPECT_EQ(*partial, 1u);
  auto message = subscriber.try_recv();
  ASSERT_TRUE(message);
  EXPECT_EQ(message->sequence, 5u);
  EXPECT_EQ(decode(message->payload).sequence, 5u);
  subscriber.release(*message);
  EXPECT_FALSE(subscriber.try_recv());
}

TEST(ChannelApiTest, LatestMpmcBatchIsWatermarkBoundedAndWraps) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto created = TypedChannel::create(config_for(salias::Mode::LatestMpmc, "latest-batch-wrap"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array<std::array<std::byte, sizeof(Payload)>, 200> storage{};
  std::array<std::span<const std::byte>, 200> payloads{};
  for (std::uint32_t index = 0; index < storage.size(); ++index) {
    storage[index] = encode(Payload{.sequence = index});
    payloads[index] = storage[index];
  }

  auto first_batch = publisher.offer_batch(payloads);
  ASSERT_TRUE(first_batch);
  EXPECT_EQ(first_batch.value(), 128u);
  for (std::uint32_t sequence = 0; sequence < 128; ++sequence) {
    storage[sequence] = encode(Payload{.sequence = 128 + sequence});
    payloads[sequence] = storage[sequence];
  }
  auto second_batch = publisher.offer_batch(payloads);
  ASSERT_TRUE(second_batch);
  EXPECT_EQ(second_batch.value(), 128u);
  for (std::uint32_t sequence = 0; sequence < 128; ++sequence) {
    storage[sequence] = encode(Payload{.sequence = 256 + sequence});
    payloads[sequence] = storage[sequence];
  }
  auto third_batch = publisher.offer_batch(payloads);
  ASSERT_TRUE(third_batch);
  EXPECT_EQ(third_batch.value(), 128u);

  std::uint32_t received = 0;
  std::uint32_t expected_sequence = 256;
  while (auto message = subscriber.try_recv()) {
    EXPECT_EQ(decode(message->payload).sequence, expected_sequence++);
    ++received;
    subscriber.release(*message);
  }
  EXPECT_EQ(received, 128u);
  EXPECT_EQ(expected_sequence, 384u);
}

TEST(ChannelApiTest, LatestMpmcEvictsMixedFramesOnFrameBoundariesAfterWrap) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto created =
      TypedChannel::create(config_for(salias::Mode::LatestMpmc, "latest-mixed-frame-wrap"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  constexpr std::array<std::size_t, 6> payload_sizes{8, 9, 16, 31, 64, 127};
  constexpr std::uint32_t kMessages = 256;
  std::array<std::vector<std::byte>, kMessages> payloads{};
  std::array<std::size_t, kMessages> frame_lengths{};
  for (std::uint32_t sequence = 0; sequence < kMessages; ++sequence) {
    const std::size_t size = payload_sizes[sequence % payload_sizes.size()];
    payloads[sequence].resize(size);
    std::memcpy(payloads[sequence].data(), &sequence, sizeof(sequence));
    frame_lengths[sequence] = 8 + ((size + 7) & ~std::size_t{7});
    ASSERT_TRUE(publisher.offer(payloads[sequence])) << sequence;
  }

  constexpr std::size_t kRetentionBytes = 4096 / 2;
  std::size_t retained_bytes = 0;
  std::uint32_t expected_sequence = kMessages;
  while (expected_sequence > 0 &&
         retained_bytes + frame_lengths[expected_sequence - 1] <= kRetentionBytes) {
    --expected_sequence;
    retained_bytes += frame_lengths[expected_sequence];
  }

  std::uint32_t received = 0;
  while (auto message = subscriber.try_recv()) {
    ASSERT_GE(message->payload.size(), sizeof(std::uint32_t));
    std::uint32_t sequence = 0;
    std::memcpy(&sequence, message->payload.data(), sizeof(sequence));
    EXPECT_EQ(sequence, expected_sequence++);
    ++received;
    subscriber.release(*message);
  }
  EXPECT_GT(received, 0u);
  EXPECT_EQ(expected_sequence, kMessages);
}

TEST(ChannelApiTest, LatestMpmcKeepsFastAndSlowSubscribersIndependent) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto config = config_for(salias::Mode::LatestMpmc, "latest-fast-slow-consumers");
  config.num_consumers = 2;
  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto fast_subscriber = channel.subscriber();
  auto slow_subscriber = channel.subscriber();

  for (std::uint32_t sequence = 0; sequence < 256; ++sequence) {
    ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = sequence}))) << sequence;
    auto message = fast_subscriber.try_recv();
    ASSERT_TRUE(message) << sequence;
    EXPECT_EQ(decode(message->payload).sequence, sequence);
    fast_subscriber.release(*message);
  }

  auto latest = slow_subscriber.try_recv();
  ASSERT_TRUE(latest);
  EXPECT_EQ(decode(latest->payload).sequence, 128u);
  slow_subscriber.release(*latest);

  auto fast_empty = fast_subscriber.try_recv();
  EXPECT_FALSE(fast_empty);
}

TEST(ChannelApiTest, LatestMpmcPinnedRingDoesNotBlockOtherProducers) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto config = config_for(salias::Mode::LatestMpmc, "latest-independent-rings");
  config.num_producers = 2;
  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  std::array publishers{channel.publisher(), channel.publisher()};
  auto subscriber = channel.subscriber();
  ASSERT_TRUE(publishers[0].offer(encode(Payload{})));
  auto held = subscriber.try_recv();
  ASSERT_TRUE(held);
  for (std::uint32_t sequence = 1; sequence < 256; ++sequence) {
    ASSERT_TRUE(publishers[0].offer(encode(Payload{.sequence = sequence})));
  }
  EXPECT_FALSE(publishers[0].offer(encode(Payload{.sequence = 256})));
  ASSERT_TRUE(publishers[1].offer(encode(Payload{.producer = 1})));
  auto other = subscriber.try_recv();
  ASSERT_TRUE(other);
  EXPECT_EQ(other->producer_id, 1u);
  EXPECT_EQ(decode(other->payload).producer, 1u);
  subscriber.release(*other);
  EXPECT_FALSE(publishers[0].offer(encode(Payload{.sequence = 256})));
  EXPECT_EQ(decode(held->payload).sequence, 0u);
  subscriber.release(*held);
  EXPECT_TRUE(publishers[0].offer(encode(Payload{.sequence = 256})));
}

TEST(ChannelApiTest, LatestMpmcDestroyingOneSubscriberDoesNotReleaseAnotherLease) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto config = config_for(salias::Mode::LatestMpmc, "latest-lease-lifetime");
  config.num_consumers = 2;
  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  std::optional first{channel.subscriber()};
  std::optional second{channel.subscriber()};
  ASSERT_TRUE(publisher.offer(encode(Payload{})));
  ASSERT_TRUE(first->try_recv());
  auto second_message = second->try_recv();
  ASSERT_TRUE(second_message);
  for (std::uint32_t sequence = 1; sequence < 256; ++sequence) {
    ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = sequence})));
  }
  first.reset();
  EXPECT_FALSE(publisher.offer(encode(Payload{.sequence = 256})));
  EXPECT_EQ(decode(second_message->payload).sequence, 0u);
  second.reset();
  EXPECT_TRUE(publisher.offer(encode(Payload{.sequence = 256})));

  auto replacement = channel.try_subscriber();
  ASSERT_TRUE(replacement);
  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 257})));
  auto latest = replacement->try_recv();
  ASSERT_TRUE(latest);
  EXPECT_EQ(decode(latest->payload).sequence, 257u);
  replacement->release(*latest);
}

template <std::uint32_t Watermark>
void check_latest_concurrent_delivery() {
  using TypedChannel = salias::LatestMpmcChannel<4096, Watermark>;
  constexpr std::uint32_t kProducers = 3;
  constexpr std::uint32_t kMessages = 10000;
  struct CheckedPayload {
    std::uint32_t producer = 0;
    std::uint32_t sequence = 0;
    std::array<std::uint64_t, 7> checks{};
  };
  auto config = config_for(salias::Mode::LatestMpmc, "latest-concurrent-delivery");
  config.num_producers = kProducers;
  config.num_consumers = 2;
  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  std::array publishers{channel.publisher(), channel.publisher(), channel.publisher()};
  std::array subscribers{channel.subscriber(), channel.subscriber()};
  std::atomic<bool> start{false};
  std::atomic<bool> stop{false};
  std::atomic<bool> failed{false};
  std::atomic<std::uint32_t> producers_done{0};
  std::atomic<std::uint32_t> consumers_done{0};
  std::vector<std::thread> workers;
  for (std::uint32_t producer = 0; producer < kProducers; ++producer) {
    workers.emplace_back([&, producer] {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (std::uint32_t sequence = 0;
           sequence < kMessages && !stop.load(std::memory_order_relaxed);) {
        CheckedPayload payload{.producer = producer, .sequence = sequence};
        payload.checks.fill((std::uint64_t{sequence} << 32) | producer);
        auto offered = publishers[producer].offer(std::as_bytes(std::span{&payload, 1}));
        if (offered) {
          ++sequence;
        } else if (offered.error() == salias::Error::BackPressured) {
          std::this_thread::yield();
        } else {
          failed.store(true, std::memory_order_relaxed);
          stop.store(true, std::memory_order_relaxed);
        }
      }
      producers_done.fetch_add(1, std::memory_order_release);
    });
  }
  for (std::size_t index = 0; index < subscribers.size(); ++index) {
    workers.emplace_back([&, index] {
      std::array<std::optional<std::uint32_t>, kProducers> last{};
      const auto validate = [&](const salias::Message& message) noexcept {
        CheckedPayload payload{};
        bool valid = message.payload.size() == sizeof(payload);
        if (valid) {
          std::memcpy(&payload, message.payload.data(), sizeof(payload));
          valid = payload.producer < kProducers && message.producer_id == payload.producer &&
                  message.sequence == payload.sequence && payload.sequence < kMessages;
        }
        if (valid) {
          const auto& previous = last[payload.producer];
          valid = !previous || payload.sequence > *previous;
          const auto expected = (std::uint64_t{payload.sequence} << 32) | payload.producer;
          for (const auto check : payload.checks) {
            valid = valid && check == expected;
          }
        }
        // Give writers an opportunity to wrap while this payload is still borrowed.
        if ((payload.sequence % 31) == 0) {
          std::this_thread::yield();
        }
        CheckedPayload again{};
        if (message.payload.size() == sizeof(again)) {
          std::memcpy(&again, message.payload.data(), sizeof(again));
          valid = valid && std::memcmp(&payload, &again, sizeof(payload)) == 0;
        }
        if (!valid) {
          failed.store(true, std::memory_order_relaxed);
          stop.store(true, std::memory_order_relaxed);
        } else {
          last[payload.producer] = payload.sequence;
        }
      };
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      while (!stop.load(std::memory_order_relaxed)) {
        bool received = false;
        if (index == 0) {
          if (auto message = subscribers[index].try_recv()) {
            validate(*message);
            subscribers[index].release(*message);
            received = true;
          }
        } else {
          received = subscribers[index].poll(7, validate) != 0;
        }
        if (std::ranges::all_of(last, [&](const auto& sequence) {
              return sequence && *sequence == kMessages - 1;
            })) {
          break;
        }
        if (!received) {
          std::this_thread::yield();
        }
      }
      consumers_done.fetch_add(1, std::memory_order_release);
    });
  }
  start.store(true, std::memory_order_release);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (consumers_done.load(std::memory_order_acquire) != subscribers.size() &&
         !stop.load(std::memory_order_relaxed) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const bool completed = consumers_done.load(std::memory_order_acquire) == subscribers.size();
  stop.store(true, std::memory_order_relaxed);
  for (auto& worker : workers) {
    worker.join();
  }
  EXPECT_TRUE(completed) << "LatestMpmc delivery did not finish before the deadline";
  EXPECT_FALSE(failed.load(std::memory_order_relaxed));
  EXPECT_EQ(producers_done.load(std::memory_order_acquire), kProducers);
}

TEST(ChannelApiTest, LatestMpmcConcurrentSingleAndBatchReadersSurviveWraparound) {
  check_latest_concurrent_delivery<50>();
  check_latest_concurrent_delivery<90>();
  check_latest_concurrent_delivery<100>();
}

TEST(ChannelApiTest, LatestMpmcPollReleasesBatchLease) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto config = config_for(salias::Mode::LatestMpmc, "latest-poll-release");
  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  for (std::uint32_t sequence = 0; sequence < 256; ++sequence) {
    ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = sequence}))) << sequence;
  }

  std::uint32_t expected_sequence = 128;
  EXPECT_EQ(subscriber.poll(2,
                            [&expected_sequence](const salias::Message& message) noexcept {
                              EXPECT_EQ(decode(message.payload).sequence, expected_sequence++);
                            }),
            2u);
  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 256})));
}

TEST(ChannelApiTest, LatestMpmcCallbackPollPinsWholeBatchUntilAllCallbacksFinish) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto created = TypedChannel::create(config_for(salias::Mode::LatestMpmc, "latest-callback-pin"));
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  for (std::uint32_t sequence = 0; sequence < 256; ++sequence) {
    ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = sequence})));
  }
  struct Context {
    salias::LatestMpmcPublisher* publisher;
    std::uint32_t callbacks = 0;
    bool writes_ok = true;
    bool blocked = false;
  } context{.publisher = &publisher};
  const auto callback = [](const salias::Message& message, void* user) noexcept {
    auto& context = *static_cast<Context*>(user);
    if (context.callbacks == 0) {
      for (std::uint32_t sequence = 256; sequence < 384; ++sequence) {
        context.writes_ok =
            context.writes_ok &&
            context.publisher->offer(encode(Payload{.sequence = sequence})).has_value();
      }
      auto offered = context.publisher->offer(encode(Payload{.sequence = 384}));
      context.blocked = !offered && offered.error() == salias::Error::BackPressured;
    }
    EXPECT_EQ(decode(message.payload).sequence, 128u + context.callbacks);
    ++context.callbacks;
  };
  EXPECT_EQ(subscriber.poll(0, callback, &context), 0u);
  EXPECT_EQ(subscriber.poll(32, nullptr, &context), 0u);
  EXPECT_EQ(subscriber.poll(32, callback, &context), 32u);
  EXPECT_TRUE(context.writes_ok);
  EXPECT_TRUE(context.blocked);
  EXPECT_EQ(context.callbacks, 32u);
  EXPECT_TRUE(publisher.offer(encode(Payload{.sequence = 384})));
}

TEST(ChannelApiTest, LatestMpmcSkipsOldMessagesAcrossProcesses) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto config = config_for(salias::Mode::LatestMpmc, "latest-fork-subscriber");
  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  auto owner = std::move(*created);

  int ready_pipe[2]{};
  int release_pipe[2]{};
  ASSERT_EQ(::pipe(ready_pipe), 0);
  ASSERT_EQ(::pipe(release_pipe), 0);

  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(ready_pipe[0]));
    static_cast<void>(::close(release_pipe[1]));
    auto connected = TypedChannel::connect(config.name);
    if (!connected) {
      ::_exit(10);
    }
    auto subscriber = connected->subscriber();
    const char ready = 'r';
    if (::write(ready_pipe[1], &ready, sizeof(ready)) != sizeof(ready)) {
      ::_exit(11);
    }
    char release = 0;
    if (::read(release_pipe[0], &release, sizeof(release)) != sizeof(release)) {
      ::_exit(12);
    }
    auto message = wait_for_message(subscriber);
    if (message.payload.empty() || decode(message.payload).sequence != 172u) {
      ::_exit(13);
    }
    subscriber.release(message);
    ::_exit(0);
  }

  static_cast<void>(::close(ready_pipe[1]));
  static_cast<void>(::close(release_pipe[0]));
  char ready = 0;
  ASSERT_EQ(::read(ready_pipe[0], &ready, sizeof(ready)), sizeof(ready));
  ASSERT_EQ(ready, 'r');

  auto publisher = owner.publisher();
  bool all_published = true;
  for (std::uint32_t sequence = 0; sequence < 300; ++sequence) {
    if (!publisher.offer(encode(Payload{.sequence = sequence}))) {
      all_published = false;
      break;
    }
  }

  const char release = 'x';
  const ssize_t release_bytes = ::write(release_pipe[1], &release, sizeof(release));
  static_cast<void>(::close(ready_pipe[0]));
  static_cast<void>(::close(release_pipe[1]));

  int child_status = 0;
  ASSERT_EQ(::waitpid(child, &child_status, 0), child);
  EXPECT_TRUE(all_published);
  EXPECT_EQ(release_bytes, sizeof(release));
  ASSERT_TRUE(WIFEXITED(child_status));
  EXPECT_EQ(WEXITSTATUS(child_status), 0);
}

TEST(ChannelApiTest, LatestMpmcReclaimsLeaseFromExitedConsumer) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto config = config_for(salias::Mode::LatestMpmc, "latest-dead-lease");
  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  auto owner = std::move(*created);
  auto publisher = owner.publisher();

  int registered_pipe[2]{};
  int acquired_pipe[2]{};
  int exit_pipe[2]{};
  ASSERT_EQ(::pipe(registered_pipe), 0);
  ASSERT_EQ(::pipe(acquired_pipe), 0);
  ASSERT_EQ(::pipe(exit_pipe), 0);

  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(registered_pipe[0]));
    static_cast<void>(::close(acquired_pipe[0]));
    static_cast<void>(::close(exit_pipe[1]));
    auto connected = TypedChannel::connect(config.name);
    if (!connected) {
      ::_exit(10);
    }
    auto subscriber = connected->subscriber();
    const char registered = 'r';
    if (::write(registered_pipe[1], &registered, sizeof(registered)) != sizeof(registered)) {
      ::_exit(11);
    }
    auto held = wait_for_message(subscriber);
    if (held.payload.empty() || decode(held.payload).sequence != 0u) {
      ::_exit(12);
    }
    const char acquired = 'a';
    if (::write(acquired_pipe[1], &acquired, sizeof(acquired)) != sizeof(acquired)) {
      ::_exit(13);
    }
    char exit = 0;
    if (::read(exit_pipe[0], &exit, sizeof(exit)) != sizeof(exit)) {
      ::_exit(14);
    }
    ::_exit(0);
  }

  static_cast<void>(::close(registered_pipe[1]));
  static_cast<void>(::close(acquired_pipe[1]));
  static_cast<void>(::close(exit_pipe[0]));
  char registered = 0;
  ASSERT_EQ(::read(registered_pipe[0], &registered, sizeof(registered)), sizeof(registered));
  ASSERT_EQ(registered, 'r');
  static_cast<void>(::close(registered_pipe[0]));
  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 0})));
  char acquired = 0;
  const ssize_t acquired_bytes = ::read(acquired_pipe[0], &acquired, sizeof(acquired));

  bool acquired_lease = acquired_bytes == sizeof(acquired) && acquired == 'a';
  std::uint32_t resume_sequence = 1;
  bool blocked_by_lease = false;
  for (; acquired_lease && resume_sequence < 256; ++resume_sequence) {
    auto offered = publisher.offer(encode(Payload{.sequence = resume_sequence}));
    if (!offered) {
      blocked_by_lease = offered.error() == salias::Error::BackPressured;
      break;
    }
  }
  if (acquired_lease && !blocked_by_lease) {
    auto offered = publisher.offer(encode(Payload{.sequence = resume_sequence}));
    blocked_by_lease = !offered && offered.error() == salias::Error::BackPressured;
  }

  const char exit = 'x';
  const ssize_t exit_bytes = ::write(exit_pipe[1], &exit, sizeof(exit));
  static_cast<void>(::close(acquired_pipe[0]));
  static_cast<void>(::close(exit_pipe[1]));

#if defined(__linux__)
  siginfo_t child_info{};
  ASSERT_EQ(::waitid(P_PID, static_cast<id_t>(child), &child_info, WEXITED | WNOWAIT), 0);
  ASSERT_EQ(child_info.si_code, CLD_EXITED);
  ASSERT_EQ(child_info.si_status, 0);
#else
  int child_status = 0;
  ASSERT_EQ(::waitpid(child, &child_status, 0), child);
  ASSERT_TRUE(WIFEXITED(child_status));
  ASSERT_EQ(WEXITSTATUS(child_status), 0);
#endif
  EXPECT_EQ(exit_bytes, sizeof(exit));
  EXPECT_TRUE(acquired_lease);
  EXPECT_TRUE(blocked_by_lease);
  EXPECT_EQ(owner.connected_consumer_count(), 0u);

  auto replacement = owner.try_subscriber();
  ASSERT_TRUE(replacement);
  EXPECT_EQ(owner.connected_consumer_count(), 1u);
  auto resumed = publisher.offer(encode(Payload{.sequence = resume_sequence}));
  ASSERT_TRUE(resumed);
  auto next = replacement->try_recv();
  ASSERT_TRUE(next);
  EXPECT_EQ(decode(next->payload).sequence, resume_sequence);
  replacement->release(*next);
#if defined(__linux__)
  int child_status = 0;
  ASSERT_EQ(::waitpid(child, &child_status, 0), child);
  ASSERT_TRUE(WIFEXITED(child_status));
  EXPECT_EQ(WEXITSTATUS(child_status), 0);
#endif
}

TEST(ChannelApiTest, LatestMpmcReclaimsLeaseAfterConsumerIsKilled) {
  using TypedChannel = salias::LatestMpmcChannel<4096, 50>;
  auto config = config_for(salias::Mode::LatestMpmc, "latest-killed-consumer");
  auto created = TypedChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  int registered_pipe[2]{};
  int acquired_pipe[2]{};
  ASSERT_EQ(::pipe(registered_pipe), 0);
  ASSERT_EQ(::pipe(acquired_pipe), 0);
  const auto child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(registered_pipe[0]));
    static_cast<void>(::close(acquired_pipe[0]));
    auto connected = TypedChannel::connect(config.name);
    if (!connected) {
      ::_exit(10);
    }
    auto subscriber = connected->subscriber();
    const char registered = 'r';
    if (::write(registered_pipe[1], &registered, sizeof(registered)) != sizeof(registered)) {
      ::_exit(11);
    }
    const auto held = wait_for_message(subscriber);
    if (held.payload.empty() || decode(held.payload).sequence != 0u) {
      ::_exit(12);
    }
    const char acquired = 'a';
    if (::write(acquired_pipe[1], &acquired, sizeof(acquired)) != sizeof(acquired)) {
      ::_exit(13);
    }
    for (;;) {
      ::pause();
    }
  }
  static_cast<void>(::close(registered_pipe[1]));
  static_cast<void>(::close(acquired_pipe[1]));
  char registered = 0;
  const auto registered_bytes = ::read(registered_pipe[0], &registered, sizeof(registered));
  static_cast<void>(::close(registered_pipe[0]));
  ASSERT_EQ(registered_bytes, sizeof(registered));
  ASSERT_EQ(registered, 'r');
  ASSERT_TRUE(publisher.offer(encode(Payload{})));
  char acquired = 0;
  const auto acquired_bytes = ::read(acquired_pipe[0], &acquired, sizeof(acquired));
  static_cast<void>(::close(acquired_pipe[0]));
  bool acquired_lease = acquired_bytes == sizeof(acquired) && acquired == 'a';
  std::uint32_t resume_sequence = 1;
  bool blocked_by_lease = false;
  for (; acquired_lease && resume_sequence < 256; ++resume_sequence) {
    auto offered = publisher.offer(encode(Payload{.sequence = resume_sequence}));
    if (!offered) {
      blocked_by_lease = offered.error() == salias::Error::BackPressured;
      break;
    }
  }
  auto blocked = publisher.offer(encode(Payload{.sequence = resume_sequence}));
  blocked_by_lease = !blocked && blocked.error() == salias::Error::BackPressured;
  const auto killed = ::kill(child, SIGKILL);
  int status = 0;
  const auto waited = ::waitpid(child, &status, 0);
  ASSERT_EQ(killed, 0);
  ASSERT_EQ(waited, child);
  ASSERT_TRUE(WIFSIGNALED(status));
  EXPECT_EQ(WTERMSIG(status), SIGKILL);
  EXPECT_TRUE(acquired_lease);
  EXPECT_TRUE(blocked_by_lease);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::Error::BackPressured);
  EXPECT_EQ(channel.connected_consumer_count(), 0u);
  auto replacement = channel.try_subscriber();
  ASSERT_TRUE(replacement);
  EXPECT_TRUE(publisher.offer(encode(Payload{.sequence = resume_sequence})));
  auto next = replacement->try_recv();
  ASSERT_TRUE(next);
  EXPECT_EQ(decode(next->payload).sequence, resume_sequence);
  replacement->release(*next);
}

/// @brief 验证非法的 producer/consumer 数量会被拒绝：零 producer、
///   单消费者模式（OrderedMpsc）配置多消费者、fanout 消费者超上限。
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

  auto fanout_config = config_for(salias::Mode::FifoMpmc, "bad-fanout-consumers");
  fanout_config.num_consumers = 9;
  auto too_many_consumers = salias::FifoMpmcChannel::create(fanout_config);
  ASSERT_FALSE(too_many_consumers);
  EXPECT_EQ(too_many_consumers.error(), salias::Error::BadConfig);
}

TEST(ChannelApiTest, DropOldestSubscriberPolicyIsRestrictedToLatestMpmc) {
  auto config = config_for(salias::Mode::FifoMpmc, "drop-oldest-policy-invalid-mode");
  auto created = salias::FifoMpmcChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);

  auto rejected = channel.try_subscriber(salias::SubscriptionPolicy::DropOldest);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), salias::Error::BadConfig);
  EXPECT_EQ(channel.connected_consumer_count(), 0u);
  EXPECT_TRUE(channel.try_subscriber());

  auto latest_config = config_for(salias::Mode::LatestMpmc, "drop-oldest-policy-invalid-value");
  auto latest = salias::LatestMpmcChannel<4096>::create(latest_config);
  ASSERT_TRUE(latest);
  auto invalid = latest->try_subscriber(static_cast<salias::SubscriptionPolicy>(99));
  ASSERT_FALSE(invalid);
  EXPECT_EQ(invalid.error(), salias::Error::BadConfig);
  EXPECT_EQ(latest->connected_consumer_count(), 0u);
}

/// @brief 验证订阅槽位耗尽可显式报告，且最后一个端点副本销毁后可复用。
TEST(ChannelApiTest, SubscriberSlotsReportExhaustionAndAreReusedAfterLastCopyDies) {
  auto config = config_for(salias::Mode::FifoMpmc, "slot-exhaustion");
  config.num_producers = 1;
  config.num_consumers = 1;
  auto created = salias::FifoMpmcChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(created).value();
  auto first_publisher = channel.publisher();
  auto exhausted_publisher = channel.publisher();
  auto first_subscriber = channel.subscriber();
  auto subscriber_copy = first_subscriber;
  auto exhausted_subscriber = channel.try_subscriber();
  ASSERT_FALSE(exhausted_subscriber);
  EXPECT_EQ(exhausted_subscriber.error(), salias::Error::NoSubscriberSlots);
  auto legacy_subscriber = channel.subscriber();
  EXPECT_FALSE(legacy_subscriber.try_recv());
  auto invalid_receive = legacy_subscriber.try_recv_checked();
  ASSERT_FALSE(invalid_receive);
  EXPECT_EQ(invalid_receive.error(), salias::Error::BadConfig);

  ASSERT_TRUE(first_publisher.offer(encode(Payload{.sequence = 1})));
  auto rejected = exhausted_publisher.offer(encode(Payload{.sequence = 2}));
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), salias::Error::BadConfig);

  auto message = first_subscriber.try_recv();
  ASSERT_TRUE(message);
  first_subscriber.release(*message);
  first_subscriber = {};
  EXPECT_EQ(channel.connected_consumer_count(), 1u);
  EXPECT_FALSE(channel.try_subscriber());
  subscriber_copy = {};
  EXPECT_EQ(channel.connected_consumer_count(), 0u);

  auto replacement = channel.try_subscriber();
  ASSERT_TRUE(replacement);
  ASSERT_TRUE(first_publisher.offer(encode(Payload{.sequence = 3})));
  auto next = replacement->try_recv();
  ASSERT_TRUE(next);
  EXPECT_EQ(decode(next->payload).sequence, 3u);
  replacement->release(*next);
}

/// @brief fork 子进程析构继承的 endpoint 时，不得注销父进程的订阅槽。
TEST(ChannelApiTest, ForkedCopyDoesNotDeregisterParentSubscriber) {
  auto config = config_for(salias::Mode::FifoMpmc, "forked-subscriber-copy");
  config.num_consumers = 1;
  auto created = salias::FifoMpmcChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  int exited_pipe[2]{};
  ASSERT_EQ(::pipe(exited_pipe), 0);
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(exited_pipe[0]));
    subscriber = {};
    const char exited = 'x';
    const auto written = ::write(exited_pipe[1], &exited, sizeof(exited));
    ::_exit(written == sizeof(exited) ? 0 : 10);
  }

  static_cast<void>(::close(exited_pipe[1]));
  char exited = 0;
  ASSERT_EQ(::read(exited_pipe[0], &exited, sizeof(exited)), sizeof(exited));
  EXPECT_EQ(exited, 'x');
  static_cast<void>(::close(exited_pipe[0]));
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);

  EXPECT_EQ(channel.connected_consumer_count(), 1u);
  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 42})));
  auto message = subscriber.try_recv();
  ASSERT_TRUE(message);
  EXPECT_EQ(decode(message->payload).sequence, 42u);
  subscriber.release(*message);
}

/// @brief 注册新订阅者时从稳定 reservation 尾开始，不重放加入前的数据。
TEST(ChannelApiTest, FanoutLateJoinStartsAtReservationBoundary) {
  auto config = config_for(salias::Mode::FifoMpmc, "fanout-late-join");
  auto created = salias::FifoMpmcChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  for (std::uint32_t sequence = 0; sequence < 8; ++sequence) {
    ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = sequence})));
  }

  auto subscriber = channel.subscriber();
  EXPECT_FALSE(subscriber.try_recv());
  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 8})));
  auto next = subscriber.try_recv();
  ASSERT_TRUE(next);
  EXPECT_EQ(decode(next->payload).sequence, 8u);
  subscriber.release(*next);
}

/// @brief OrderedMpmc late join 从全局序列快照开始，并保留新消息的跨 ring 次序。
TEST(ChannelApiTest, OrderedMpmcLateJoinStartsAtStableGlobalSequence) {
  auto config = config_for(salias::Mode::OrderedMpmc, "ordered-fanout-late-join");
  config.num_producers = 2;
  config.num_consumers = 2;
  auto created = salias::OrderedMpmcChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  std::array publishers{channel.publisher(), channel.publisher()};
  for (std::uint32_t sequence = 0; sequence < 4; ++sequence) {
    ASSERT_TRUE(publishers[sequence % publishers.size()].offer(
        encode(Payload{.producer = sequence % 2, .sequence = sequence})));
  }

  auto subscriber = channel.subscriber();
  EXPECT_FALSE(subscriber.try_recv());
  ASSERT_TRUE(publishers[1].offer(encode(Payload{.producer = 1, .sequence = 4})));
  ASSERT_TRUE(publishers[0].offer(encode(Payload{.producer = 0, .sequence = 5})));
  for (std::uint64_t sequence = 4; sequence < 6; ++sequence) {
    auto message = subscriber.try_recv();
    ASSERT_TRUE(message);
    EXPECT_EQ(message->sequence, sequence);
    EXPECT_EQ(decode(message->payload).sequence, sequence);
    subscriber.release(*message);
  }
  EXPECT_FALSE(subscriber.try_recv());
}

/// @brief 旧进程继承的订阅端在槽位复用后不能操作新代际的游标。
TEST(ChannelApiTest, ReusedSlotRejectsEndpointInheritedByOlderProcess) {
  auto config = config_for(salias::Mode::FifoMpmc, "stale-subscriber-generation");
  config.num_consumers = 1;
  auto created = salias::FifoMpmcChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  auto publisher = channel.publisher();
  auto old_subscriber = channel.subscriber();

  int start_pipe[2]{};
  int checked_pipe[2]{};
  ASSERT_EQ(::pipe(start_pipe), 0);
  ASSERT_EQ(::pipe(checked_pipe), 0);
  const auto child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(start_pipe[1]));
    static_cast<void>(::close(checked_pipe[0]));
    char start = 0;
    if (::read(start_pipe[0], &start, sizeof(start)) != sizeof(start) || start != 's') {
      ::_exit(10);
    }
    const bool received = old_subscriber.try_recv().has_value();
    const char checked = received ? 'x' : 'c';
    if (::write(checked_pipe[1], &checked, sizeof(checked)) != sizeof(checked)) {
      ::_exit(11);
    }
    ::_exit(received ? 12 : 0);
  }
  static_cast<void>(::close(start_pipe[0]));
  static_cast<void>(::close(checked_pipe[1]));
  old_subscriber = {};
  auto replacement = channel.try_subscriber();
  ASSERT_TRUE(replacement);
  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 42})));
  const char start = 's';
  ASSERT_EQ(::write(start_pipe[1], &start, sizeof(start)), sizeof(start));
  char checked = 0;
  ASSERT_EQ(::read(checked_pipe[0], &checked, sizeof(checked)), sizeof(checked));
  EXPECT_EQ(checked, 'c');
  static_cast<void>(::close(start_pipe[1]));
  static_cast<void>(::close(checked_pipe[0]));
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);

  auto current = replacement->try_recv();
  ASSERT_TRUE(current);
  EXPECT_EQ(decode(current->payload).sequence, 42u);
  replacement->release(*current);
}

/// @brief 并发发布期间反复注册/注销订阅者不会撕裂新订阅边界或阻塞发布。
TEST(ChannelApiTest, ConcurrentSubscriberChurnAndPublishingRemainConsistent) {
  auto config = config_for(salias::Mode::FifoMpmc, "subscriber-churn-publish");
  config.num_producers = 2;
  config.num_consumers = 2;
  auto created = salias::FifoMpmcChannel::create(config);
  ASSERT_TRUE(created);
  auto channel = std::move(*created);
  std::array publishers{channel.publisher(), channel.publisher()};

  constexpr std::uint32_t kMessagesPerProducer = 10000;
  std::atomic<bool> stop{false};
  std::atomic<std::uint32_t> done{0};
  std::atomic<bool> failed{false};
  std::array<std::thread, 2> producers;
  for (std::uint32_t producer_id = 0; producer_id < producers.size(); ++producer_id) {
    producers[producer_id] = std::thread([&, producer_id] {
      std::uint32_t sequence = 0;
      while (sequence < kMessagesPerProducer && !stop.load(std::memory_order_relaxed)) {
        auto offered = publishers[producer_id].offer(
            encode(Payload{.producer = producer_id, .sequence = sequence}));
        if (offered) {
          ++sequence;
        } else if (offered.error() == salias::Error::BackPressured) {
          std::this_thread::yield();
        } else {
          failed.store(true, std::memory_order_relaxed);
          break;
        }
      }
      done.fetch_add(1, std::memory_order_release);
    });
  }

  auto drain = [&](salias::FifoMpmcSubscriber& subscriber, std::array<bool, 2>& have_previous,
                   std::array<std::uint32_t, 2>& previous) {
    while (auto message = subscriber.try_recv()) {
      const auto payload = decode(message->payload);
      if (message->producer_id >= have_previous.size() ||
          payload.producer != message->producer_id) {
        failed.store(true, std::memory_order_relaxed);
      } else if (have_previous[message->producer_id] &&
                 payload.sequence <= previous[message->producer_id]) {
        failed.store(true, std::memory_order_relaxed);
      } else {
        have_previous[message->producer_id] = true;
        previous[message->producer_id] = payload.sequence;
      }
      subscriber.release(*message);
    }
  };

  for (std::uint32_t generation = 0;
       generation < 256 && done.load(std::memory_order_acquire) < producers.size(); ++generation) {
    auto subscriber = channel.try_subscriber();
    if (!subscriber) {
      failed.store(true, std::memory_order_relaxed);
      break;
    }
    std::array<bool, 2> have_previous{};
    std::array<std::uint32_t, 2> previous{};
    drain(*subscriber, have_previous, previous);
    std::this_thread::yield();
  }

  auto final_subscriber = channel.try_subscriber();
  if (!final_subscriber) {
    failed.store(true, std::memory_order_relaxed);
    stop.store(true, std::memory_order_relaxed);
  } else {
    std::array<bool, 2> have_previous{};
    std::array<std::uint32_t, 2> previous{};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (done.load(std::memory_order_acquire) < producers.size() &&
           std::chrono::steady_clock::now() < deadline) {
      drain(*final_subscriber, have_previous, previous);
      std::this_thread::yield();
    }
    if (done.load(std::memory_order_acquire) < producers.size()) {
      failed.store(true, std::memory_order_relaxed);
      stop.store(true, std::memory_order_relaxed);
    }
    drain(*final_subscriber, have_previous, previous);
  }
  for (auto& producer : producers) {
    producer.join();
  }
  EXPECT_FALSE(failed.load(std::memory_order_relaxed));
  EXPECT_EQ(done.load(std::memory_order_acquire), producers.size());
}

/// @brief 验证订阅者存活数按已注册槽位和注册进程状态统计。
TEST(ChannelApiTest, ConnectedConsumerCountTracksRegisteredProcessLiveness) {
  auto config = config_for(salias::Mode::FifoMpmc, "consumer-liveness");
  config.num_consumers = 2;
  auto created = salias::FifoMpmcChannel::create(config);
  ASSERT_TRUE(created);
  auto owner = std::move(*created);
  EXPECT_EQ(owner.connected_consumer_count(), 0u);

  auto parent_subscriber = owner.subscriber();
  EXPECT_EQ(owner.connected_consumer_count(), 1u);

  int ready_pipe[2]{};
  int release_pipe[2]{};
  ASSERT_EQ(::pipe(ready_pipe), 0);
  ASSERT_EQ(::pipe(release_pipe), 0);
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::close(ready_pipe[0]));
    static_cast<void>(::close(release_pipe[1]));
    auto connected = salias::FifoMpmcChannel::connect(config.name);
    if (!connected) {
      ::_exit(10);
    }
    auto child_subscriber = connected->subscriber();
    const char ready = 'r';
    if (::write(ready_pipe[1], &ready, sizeof(ready)) != sizeof(ready)) {
      ::_exit(11);
    }
    char release = 0;
    if (::read(release_pipe[0], &release, sizeof(release)) != sizeof(release)) {
      ::_exit(12);
    }
    ::_exit(0);
  }

  static_cast<void>(::close(ready_pipe[1]));
  static_cast<void>(::close(release_pipe[0]));
  char ready = 0;
  const ssize_t ready_bytes = ::read(ready_pipe[0], &ready, sizeof(ready));
  if (ready_bytes != sizeof(ready)) {
    const char release = 'x';
    const ssize_t release_bytes = ::write(release_pipe[1], &release, sizeof(release));
    static_cast<void>(release_bytes);
    int child_status = 0;
    static_cast<void>(::waitpid(child, &child_status, 0));
    FAIL() << "child failed to register its subscriber";
  }

  EXPECT_EQ(owner.connected_consumer_count(), 2u);
  const char release = 'x';
  ASSERT_EQ(::write(release_pipe[1], &release, sizeof(release)), sizeof(release));
  int child_status = 0;
  ASSERT_EQ(::waitpid(child, &child_status, 0), child);
  EXPECT_TRUE(WIFEXITED(child_status));
  EXPECT_EQ(WEXITSTATUS(child_status), 0);
  EXPECT_EQ(owner.connected_consumer_count(), 1u);

  static_cast<void>(::close(ready_pipe[0]));
  static_cast<void>(::close(release_pipe[1]));
  static_cast<void>(parent_subscriber);
}

/// @brief 验证启用大页（huge page）时 capacity 必须与 2MB 对齐，
///   未对齐的配置应在 create 阶段被拒绝并返回 BadConfig。
TEST(ChannelApiTest, HugePageCapacityMustBeAligned) {
  auto config = config_for(salias::Mode::FifoMpsc, "huge-alignment");
  config.huge = salias::HugePage::Size2MB;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_FALSE(created);
  EXPECT_EQ(created.error(), salias::Error::BadConfig);
}

/// @brief 验证多个进程通过 fork 连接同名共享内存区域时，能正确共享
///   预分配的 producer 槽位（IPC 场景）。
/// @details 父进程创建 channel 并配置 2 个 producer 槽位，随后 fork 两个
///   子进程各自 connect 并发布一条消息。父进程 subscriber 应收到两条来自
///   不同 producer_id 的消息，验证跨进程 producer 槽位的独立分配与可见性。
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
      // 子进程：connect 同名共享内存区域并占据一个 producer 槽位发布消息。
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

/// @brief 验证公共 API 允许的最长 channel 名称可在 POSIX shm 后端创建并连接。
TEST(ChannelApiTest, LongChannelNameSupportsCreateAndConnect) {
  auto config = config_for(salias::Mode::FifoMpsc, "long-name");
  config.name.assign(128, 'a');
  const std::string process_id = std::to_string(::getpid());
  config.name.replace(config.name.size() - process_id.size(), process_id.size(), process_id);
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created);

  auto connected = salias::FifoMpscChannel::connect(config.name);
  ASSERT_TRUE(connected);

  auto owner = std::move(created).value();
  auto peer = std::move(connected).value();
  auto publisher = owner.publisher();
  auto subscriber = peer.subscriber();

  ASSERT_TRUE(publisher.offer(encode(Payload{.sequence = 42})));
  auto message = wait_for_message(subscriber);
  ASSERT_FALSE(message.payload.empty());
  EXPECT_EQ(decode(message.payload).sequence, 42u);
  subscriber.release(message);
}

#if defined(__APPLE__)
/// @brief 验证 macOS 明确拒绝 Linux hugetlbfs 显式大页后端。
TEST(ChannelApiTest, ExplicitHugePagesAreUnavailableOnMacOS) {
  auto config = config_for(salias::Mode::FifoMpsc, "macos-huge-pages");
  config.capacity = 2u * 1024u * 1024u;
  config.huge = salias::HugePage::Size2MB;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_FALSE(created);
  EXPECT_EQ(created.error(), salias::Error::PlatformFail);
}
#endif

}  // namespace
