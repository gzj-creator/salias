/** @file test/api/channel_api_test.cpp
 * @brief salias L7 公共 API（channel 层）的端到端集成测试。
 * @details 本文件位于 L5 channel / L7 salias 公共 API 层，验证各类 channel
 *   （FifoMpsc/OrderedMpsc/FifoFanout/OrderedFanout）在 claim/commit、offer、
 *   offer_batch、subscriber 轮询与 release 等关键路径上的正确性。覆盖面包括：
 *   FIFO 与全局有序两种交付语义、fanout 多消费者广播、生产者/消费者槽位
 *   耗尽与大页对齐校验，以及多进程（fork）共享 producer 槽位的 IPC 场景。
 *   线程/进程模型：单进程内通过 subscriber/publisher 对象操作，跨进程场景
 *   通过 fork 子进程 connect 同名共享内存区域实现。
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
#include <thread>
#include <type_traits>
#include <vector>

#include "salias/salias.hpp"

namespace {  // 匿名命名空间：测试内部辅助类型与函数，仅本编译单元可见。

// 编译期断言：PublishClaim 不可拷贝（独占 claim 的所有权只能移动，防止误用）。
static_assert(!std::is_copy_constructible_v<salias::PublishClaim<salias::Mode::FifoMpsc>>);
// 编译期断言：FIFO 与 Ordered 是两种独立 channel 类型，不可互换。
static_assert(!std::is_same_v<salias::FifoMpscChannel, salias::OrderedMpscChannel>);
static_assert(!std::is_same_v<salias::FifoFanoutChannel, salias::OrderedFanoutChannel>);

/// @brief 测试用负载结构体。
/// @details 模拟一条业务消息：producer 标识来源生产者编号，sequence 标识
///   消息序号，用于在测试中校验交付顺序与来源正确性。
struct Payload {
  std::uint32_t producer = 0;
  std::uint32_t sequence = 0;
};

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

/// @brief 验证 FifoFanout 模式下同一条消息会被广播到每一个 subscriber。
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

/// @brief 验证 OrderedFanout 模式下每个 subscriber 都按全局序列号顺序收到消息，
///   且后序消息在前序消息就绪前对所有 subscriber 不可见。
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

  auto fanout_config = config_for(salias::Mode::FifoFanout, "bad-fanout-consumers");
  fanout_config.num_consumers = 9;
  auto too_many_consumers = salias::FifoFanoutChannel::create(fanout_config);
  ASSERT_FALSE(too_many_consumers);
  EXPECT_EQ(too_many_consumers.error(), salias::Error::BadConfig);
}

/// @brief 验证 producer/consumer 槽位耗尽后再次申请会在使用时返回 BadConfig。
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

}  // namespace
