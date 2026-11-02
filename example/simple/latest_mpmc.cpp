/**
 * @file example/simple/latest_mpmc.cpp
 * @brief MagicRing 场景示例：LatestMpmcChannel 的有界最新窗口。
 * @details 适用场景：读者只关心最新状态，旧数据可以丢弃，例如行情快照、监控面板、
 *          设备最新状态广播。`LatestMpmcChannel` 复用通用 `Channel` 与 per-producer
 *          `MagicRing`，但用保留水位替代消费者游标背压：默认 DropOldest 订阅者会跳过
 *          已移出窗口的旧消息，慢读者不会拖住 producer。
 *
 *          语义边界：消息仍是变长 frame，容量按字节计；需要“每条都不能丢”时请用
 *          默认 Reliable 订阅策略或通用 FIFO `Channel`。需要固定大小、固定参与者时
 *          用 `FixedChannel` 与 `Fast*Ring`。
 *
 *          本示例先发送超过窗口的数据，再读取；可以看到老消息被淘汰、首条读取触发
 *          Lagged、最终只拿到窗口内的最新一段。
 */
#include <cstdint>
#include <iostream>

#include "../common.hpp"
#include "../quote.hpp"

int main() {
  // 模板参数：每个 producer ring 64 KiB，保留水位 90%。
  using Latest = salias::LatestMpmcChannel<1u << 16, 90, salias_demo::QuoteSchema>;
  using Quote = salias_demo::Quote;

  salias::Config config;
  config.name = salias_demo::unique_name("salias-example-simple-latest-mpmc");
  config.num_producers = 1;
  config.num_consumers = 1;

  auto channel = salias_demo::unwrap(Latest::create(config), "create");
  auto publisher = channel.publisher();
  // 默认订阅策略是 DropOldest；换成 SubscriptionPolicy::Reliable 会施加背压。
  auto subscriber = channel.subscriber(salias::SubscriptionPolicy::DropOldest);

  // 单条约 32 B，64 KiB 的 90% 水位大约只能保留一千多条；发送 3000 条必然发生淘汰。
  constexpr std::uint32_t kSent = 3000;
  for (std::uint32_t i = 0; i < kSent; ++i) {
    salias_demo::check(publisher.send(Quote{
                           .symbol = 600000, .price = 10'000 + i, .quantity = 100, .sequence = i}),
                       "send");
  }

  std::uint32_t received = 0;
  std::uint32_t last_sequence = 0;
  std::uint64_t lag_events = 0;
  for (;;) {
    auto next = subscriber.try_recv_copy();
    if (!next) {
      // Lagged 表示旧游标已落到保留窗口之外；推进后重试即可读最新一段。
      if (next.error() == salias::Error::Lagged) {
        ++lag_events;
        continue;
      }
      salias_demo::print_error("try_recv_copy", next.error());
      return 1;
    }
    if (!*next) {
      break;
    }
    ++received;
    last_sequence = (*next)->sequence;
  }

  std::cout << "latest sent=" << kSent << " kept=" << received << " last_seq=" << last_sequence
            << " lag_events=" << lag_events << " lagged_messages=" << subscriber.lagged_messages()
            << " skipped_messages=" << subscriber.skipped_messages() << '\n';
  return last_sequence == kSent - 1 && received < kSent ? 0 : 2;
}
