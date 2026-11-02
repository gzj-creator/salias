/**
 * @file example/simple/ordered_mpmc.cpp
 * @brief MagicRing 场景示例：OrderedMpmc 的全局全序与多订阅者 fanout。
 * @details 适用场景：多个 producer 的事件必须按同一全局顺序交付，同时每个订阅者都要
 *          收到全量消息。例如多个行情网关写入，策略端和审计端各自按相同顺序回放。
 *          底层仍是通用 `Channel` 与 per-producer `MagicRing`，但 claim 时分配全局序号，
 *          consumer 必须等待更早的全局序号发布后才能交付。
 *
 *          本示例先 claim producer 0 的消息、再 claim producer 1 的消息，让全局序号
 *          更晚的 producer 1 先 commit。两个订阅者都不会提前看到 producer 1，最终都按
 *          producer 0、producer 1 的顺序收到消息。
 *
 *          语义边界：OrderedMpmc 用共享全局序号建立跨 producer 全序，吞吐成本高于
 *          FifoMpmc。只有单 producer 的 SPSC/SPMC 形状不需要 Ordered，FIFO 已经具备
 *          全序；固定槽 `FixedChannel` 没有 Ordered 变体。
 */
#include <cstdint>
#include <cstring>
#include <thread>

#include "../quote.hpp"

namespace {

/// @brief 读取两条消息，验证两个订阅者都按同一条 producer lane 顺序收到。
template <class Subscriber>
void consume(Subscriber& subscriber, const char* label) {
  std::uint64_t previous_symbol = 0;
  for (std::uint32_t i = 0; i < 2; ++i) {
    auto next = salias_demo::unwrap(subscriber.try_recv_copy(), "try_recv_copy");
    while (!next) {
      std::this_thread::yield();
      next = salias_demo::unwrap(subscriber.try_recv_copy(), "try_recv_copy");
    }
    salias_demo::print_quote(label, *next);
    if (i == 0 && next->symbol != 600000) {
      std::exit(2);
    }
    if (i == 1 && (next->symbol != 600001 || previous_symbol != 600000)) {
      std::exit(3);
    }
    previous_symbol = next->symbol;
  }
}

}  // namespace

int main() {
  using Channel = salias::Channel<salias::Mode::OrderedMpmc, salias_demo::QuoteSchema>;
  using Quote = salias_demo::Quote;

  salias::Config config;
  config.name = salias_demo::unique_name("salias-example-simple-ordered-mpmc");
  config.num_producers = 2;
  config.num_consumers = 2;

  auto channel = salias_demo::unwrap(Channel::create(config), "create");
  auto first = channel.publisher();
  auto second = channel.publisher();
  auto strategy = channel.subscriber();
  auto audit = channel.subscriber();

  // claim 顺序决定全局序号：first 是 1，second 是 2。
  auto first_claim = salias_demo::unwrap(first.try_claim(), "first.try_claim");
  auto second_claim = salias_demo::unwrap(second.try_claim(), "second.try_claim");
  const Quote first_quote{.symbol = 600000, .price = 10'000, .quantity = 100, .sequence = 0};
  const Quote second_quote{.symbol = 600001, .price = 20'000, .quantity = 200, .sequence = 0};
  std::memcpy(first_claim.payload().data(), &first_quote, sizeof(first_quote));
  std::memcpy(second_claim.payload().data(), &second_quote, sizeof(second_quote));

  // 全局序号更晚的消息先 commit；Ordered consumer 仍需等待 first_claim。
  second.commit(std::move(second_claim));
  first.commit(std::move(first_claim));

  consume(strategy, "ordered-mpmc strategy");
  consume(audit, "ordered-mpmc audit");
  return 0;
}
