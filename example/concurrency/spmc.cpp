/**
 * @file example/concurrency/spmc.cpp
 * @brief FastSpmcRing 场景示例：单 producer、多 consumer fanout。
 * @details 适用场景：一份数据要同时送给多个本地读者，例如行情快照同时供策略、
 *          风控和监控使用。每个 consumer 有独立 cursor，都会收到全量消息，不是竞争
 *          消费；最慢的 consumer 会通过背压限制 producer，避免未读数据被覆盖。
 *
 *          底层链路：`FixedChannel<FixedTopology::Spmc>` -> `FastSpmcRing`。
 *          不适用场景：只允许一个 reader、希望多个 reader 分担消息（负载均衡），或
 *          需要跨 producer 全局顺序。
 */
#include <cstdint>
#include <thread>

#include "../quote.hpp"

namespace {

/// @brief 消费指定数量的消息并打印；每个 consumer 都从自己的 cursor 开始。
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
  using Channel =
      salias::FixedChannel<salias::FixedTopology::Spmc, salias_demo::QuoteSchema, 1024, 1, 2>;
  using Quote = salias_demo::Quote;

  const std::string name = salias_demo::unique_name("salias-example-concurrency-spmc");
  auto channel = salias_demo::unwrap(Channel::create(name), "create");

  auto publisher = salias_demo::unwrap(channel.publisher(0), "publisher(0)");
  auto strategy = salias_demo::unwrap(channel.subscriber(0), "subscriber(0)");
  auto monitor = salias_demo::unwrap(channel.subscriber(1), "subscriber(1)");

  constexpr std::uint32_t kCount = 4;
  for (std::uint32_t i = 0; i < kCount; ++i) {
    salias_demo::check(publisher.send(Quote{
                           .symbol = 600000, .price = 10'000 + i, .quantity = 100, .sequence = i}),
                       "send");
  }

  // 两个 consumer 各自收全量；先读策略端，再读监控端，模拟不同处理速度。
  consume(strategy, "spmc strategy", kCount);
  consume(monitor, "spmc monitor", kCount);
  return 0;
}
