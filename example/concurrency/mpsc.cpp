/**
 * @file example/concurrency/mpsc.cpp
 * @brief FastMpscRing 场景示例：多 producer、单 consumer。
 * @details 适用场景：多个线程或进程向同一个汇总方写入，例如多路行情源汇聚到一个
 *          策略消费者。`FastMpscRing` 为每个 producer 分配独立 lane，因此 producer
 *          之间没有共享写位置竞争；单 lane 内保持 FIFO，但不同 lane 之间的到达顺序
 *          不保证全局一致。
 *
 *          底层链路：`FixedChannel<FixedTopology::Mpsc>` -> `FastMpscRing`。
 *          不适用场景：需要跨 producer 全局有序（用 `Mode::OrderedMpsc` 通用 Channel）
 *          或多个 consumer 各自收全量（用 Spmc/Mpmc）。
 */
#include <cstdint>
#include <thread>

#include "../quote.hpp"

int main() {
  using Channel =
      salias::FixedChannel<salias::FixedTopology::Mpsc, salias_demo::QuoteSchema, 1024, 2>;
  using Quote = salias_demo::Quote;

  const std::string name = salias_demo::unique_name("salias-example-concurrency-mpsc");
  auto channel = salias_demo::unwrap(Channel::create(name), "create");

  // 固定拓扑的 producer 数量由模板参数决定；索引就是 producer lane 编号。
  auto first = salias_demo::unwrap(channel.publisher(0), "publisher(0)");
  auto second = salias_demo::unwrap(channel.publisher(1), "publisher(1)");
  auto subscriber = salias_demo::unwrap(channel.subscriber(0), "subscriber(0)");

  constexpr std::uint32_t kPerProducer = 3;
  for (std::uint32_t i = 0; i < kPerProducer; ++i) {
    salias_demo::check(
        first.send(Quote{.symbol = 600000, .price = 10'000 + i, .quantity = 100, .sequence = i}),
        "first.send");
    // 故意交替写入两个 lane，输出仍应满足每个 symbol 自己的 sequence 递增。
    salias_demo::check(
        second.send(Quote{.symbol = 600001, .price = 20'000 + i, .quantity = 200, .sequence = i}),
        "second.send");
  }

  std::uint32_t received = 0;
  std::uint32_t last_600000 = 0;
  std::uint32_t last_600001 = 0;
  while (received < kPerProducer * 2) {
    auto next = salias_demo::unwrap(subscriber.try_recv_copy(), "try_recv_copy");
    if (!next) {
      std::this_thread::yield();
      continue;
    }
    salias_demo::print_quote("mpsc consumer", *next);

    // 验证语义边界：MPSC 只保证单 producer lane 内 FIFO，不保证跨 lane 全局顺序。
    std::uint32_t& last = next->symbol == 600000 ? last_600000 : last_600001;
    if (next->sequence != last) {
      return 2;
    }
    last = next->sequence + 1;
    ++received;
  }
  return 0;
}
