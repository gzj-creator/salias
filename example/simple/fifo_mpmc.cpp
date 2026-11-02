/**
 * @file example/simple/fifo_mpmc.cpp
 * @brief MagicRing 场景示例：通用 Channel 的 MPMC 多订阅者交付。
 * @details 适用场景：一份消息要同时送给多个订阅者，例如策略、风控和监控各自读取
 *          同一路事件。通用 `Channel` 的 MPMC 模式为每个订阅者维护独立 cursor，
 *          订阅者之间是 fanout 关系，不是竞争消费；最慢订阅者通过背压限制 producer。
 *
 *          与 `concurrency/mpmc.cpp` 的区别：本示例走通用 `Channel`，支持变长/固定
 *          消息、动态创建订阅端点以及 Ordered 等其他模式；`FixedChannel` 版本把参与者
 *          数量和 payload 大小固定在编译期，热路径更短。
 *
 *          是否需要全局顺序：本示例使用 `Mode::FifoMpmc`；需要跨 producer 全序时改用
 *          `Mode::OrderedMpmc`，订阅端 API 不变。
 */
#include <cstdint>
#include <thread>

#include "../quote.hpp"

namespace {

/// @brief 从指定订阅者读取 expected 条消息；每个订阅者都有自己的 cursor。
template <class Subscriber>
void consume(Subscriber& subscriber, const char* label, std::uint32_t expected) {
  std::uint32_t received = 0;
  while (received < expected) {
    auto next = salias_demo::unwrap(subscriber.try_recv_copy(), "try_recv_copy");
    if (!next) {
      std::this_thread::yield();
      continue;
    }
    salias_demo::print_quote(label, *next);
    ++received;
  }
}

}  // namespace

int main() {
  using Channel = salias::Channel<salias::Mode::FifoMpmc, salias_demo::QuoteSchema>;
  using Quote = salias_demo::Quote;

  salias::Config config;
  config.name = salias_demo::unique_name("salias-example-simple-fifo-mpmc");
  config.num_producers = 1;
  config.num_consumers = 2;

  auto channel = salias_demo::unwrap(Channel::create(config), "create");
  auto publisher = channel.publisher();
  // 每个 subscriber() 都是独立订阅端点；消息会复制给所有订阅者。
  auto strategy = channel.subscriber();
  auto audit = channel.subscriber();

  constexpr std::uint32_t kCount = 4;
  for (std::uint32_t i = 0; i < kCount; ++i) {
    salias_demo::check(publisher.send(Quote{
                           .symbol = 600000, .price = 10'000 + i, .quantity = 100, .sequence = i}),
                       "send");
  }

  // 两个订阅者都收到全量消息；读取速度不同不会互相消费对方的副本。
  consume(strategy, "fanout strategy", kCount);
  consume(audit, "fanout audit", kCount);
  return 0;
}
