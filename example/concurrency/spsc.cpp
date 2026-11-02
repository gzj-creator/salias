/**
 * @file example/concurrency/spsc.cpp
 * @brief FastSpscRing 场景示例：单 producer、单 consumer。
 * @details 适用场景：一条稳定的数据流，例如行情网关到策略引擎、采集线程到落盘线程。
 *          这是固定槽 ring 中开销最低的拓扑，写入方和读取方各自只有一个线程，
 *          不需要跨 producer 或跨 consumer 的协调。
 *
 *          底层链路：`FixedChannel<FixedTopology::Spsc>` -> `FastSpscRing`。
 *          不适用场景：多个 producer 写入、多个 consumer 各自消费、变长消息、TTL、
 *          动态订阅。需要这些能力时使用通用 `Channel`。
 *
 *          示例同时演示两条发送路径：
 *          - `send()`：一次对象复制，代码最短；
 *          - `try_claim()` + `commit()`：先拿到共享内存写入区，再提交，避免本地复制。
 */
#include <cstdint>
#include <cstring>
#include <thread>

#include "../quote.hpp"

int main() {
  using Channel = salias::FixedChannel<salias::FixedTopology::Spsc, salias_demo::QuoteSchema, 1024>;
  using Quote = salias_demo::Quote;

  // 固定通道的容量单位是 slot 数，而不是字节数；1024 表示每个 producer lane 有 1024 个槽。
  const std::string name = salias_demo::unique_name("salias-example-concurrency-spsc");
  auto channel = salias_demo::unwrap(Channel::create(name), "create");

  auto publisher = salias_demo::unwrap(channel.publisher(0), "publisher(0)");
  auto subscriber = salias_demo::unwrap(channel.subscriber(0), "subscriber(0)");

  constexpr std::uint32_t kCount = 4;
  for (std::uint32_t i = 0; i < kCount; ++i) {
    const Quote quote{.symbol = 600000, .price = 10'000 + i, .quantity = 100, .sequence = i};
    if (i == 0) {
      // 零拷贝路径：claim 返回的 payload 已经是共享 ring 中的可写区域。
      auto claim = salias_demo::unwrap(publisher.try_claim(), "try_claim");
      std::memcpy(claim.payload().data(), &quote, sizeof(quote));
      salias_demo::check(publisher.commit(std::move(claim)), "commit");
    } else {
      // 拷贝路径：send 内部完成一次对象复制。
      salias_demo::check(publisher.send(quote), "send");
    }
  }

  // try_recv_copy 每条消息立即推进 consumer cursor；没有数据时返回空 optional。
  std::uint32_t received = 0;
  while (received < kCount) {
    auto next = salias_demo::unwrap(subscriber.try_recv_copy(), "try_recv_copy");
    if (!next) {
      std::this_thread::yield();
      continue;
    }
    salias_demo::print_quote("spsc consumer", *next);
    ++received;
  }
  return 0;
}
