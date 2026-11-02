/**
 * @file example/concurrency/mpmc.cpp
 * @brief FastMpmcRing 场景示例：多 producer、多 consumer fanout。
 * @details 适用场景：多条独立写入流需要同时供多个读者使用，例如多个行情网关写入，
 *          策略和审计各自读取全量数据。每个 producer 有独立 lane，每个 consumer 有
 *          独立 cursor；consumer 之间是 fanout 关系，不是竞争消费。
 *
 *          底层链路：`FixedChannel<FixedTopology::Mpmc>` -> `FastMpmcRing`。
 *          语义边界：只保证同一 producer lane 内 FIFO，不提供跨 lane 全局顺序；
 *          最慢 consumer 决定背压。需要全局有序时使用通用 `Channel` 的 Ordered 模式。
 */
#include <cstdint>
#include <thread>

#include "../quote.hpp"

namespace {

/// @brief 从指定 consumer 读取 expected 条消息，并按 symbol 验证各 lane 的序号。
template <class Subscriber>
void consume(Subscriber& subscriber, const char* label, std::uint32_t expected) {
  std::uint32_t received = 0;
  std::uint32_t last_600000 = 0;
  std::uint32_t last_600001 = 0;
  while (received < expected) {
    auto next = salias_demo::unwrap(subscriber.try_recv_copy(), "try_recv_copy");
    if (!next) {
      std::this_thread::yield();
      continue;
    }
    salias_demo::print_quote(label, *next);
    std::uint32_t& last = next->symbol == 600000 ? last_600000 : last_600001;
    if (next->sequence != last) {
      std::exit(2);
    }
    last = next->sequence + 1;
    ++received;
  }
}

}  // namespace

int main() {
  using Channel =
      salias::FixedChannel<salias::FixedTopology::Mpmc, salias_demo::QuoteSchema, 1024, 2, 2>;
  using Quote = salias_demo::Quote;

  const std::string name = salias_demo::unique_name("salias-example-concurrency-mpmc");
  auto channel = salias_demo::unwrap(Channel::create(name), "create");

  auto first = salias_demo::unwrap(channel.publisher(0), "publisher(0)");
  auto second = salias_demo::unwrap(channel.publisher(1), "publisher(1)");
  auto strategy = salias_demo::unwrap(channel.subscriber(0), "subscriber(0)");
  auto audit = salias_demo::unwrap(channel.subscriber(1), "subscriber(1)");

  constexpr std::uint32_t kPerProducer = 3;
  for (std::uint32_t i = 0; i < kPerProducer; ++i) {
    salias_demo::check(
        first.send(Quote{.symbol = 600000, .price = 10'000 + i, .quantity = 100, .sequence = i}),
        "first.send");
    salias_demo::check(
        second.send(Quote{.symbol = 600001, .price = 20'000 + i, .quantity = 200, .sequence = i}),
        "second.send");
  }

  // 两个 consumer 都收到两个 lane 的全量消息；读取顺序可以不同。
  consume(strategy, "mpmc strategy", kPerProducer * 2);
  consume(audit, "mpmc audit", kPerProducer * 2);
  return 0;
}
