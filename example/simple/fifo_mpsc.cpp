/**
 * @file example/simple/fifo_mpsc.cpp
 * @brief MagicRing 场景示例：FIFO MPSC 的提交顺序语义。
 * @details 适用场景：多个 producer 写同一通道，只要求各自 lane 内先进先出，不要求
 *          跨 producer 的全局顺序；典型如日志采集、指标上报。底层是通用 `Channel`，
 *          由 per-producer `MagicRing` 承载变长 frame。
 *
 *          本示例让 producer 0 先 claim 但暂不 commit，producer 1 随后发布。consumer
 *          会先看到 producer 1 的消息，说明 FIFO 的“顺序”是提交流程顺序，而不是 claim
 *          顺序，也不建立跨 producer 的提交全序。
 *
 *          如果需要跨 producer 全局有序，请参考 `ordered_mpsc.cpp`；如果消息大小固定
 *          且参与者固定，追求更低热路径开销，请使用 `FixedChannel` 与 `Fast*Ring`
 *          示例（`example/concurrency/`）。
 */
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <salias/salias.hpp>
#include <span>
#include <string>
#include <utility>

int main() {
  salias::Config config;
  config.name = "salias-minimal-fifo-" + std::to_string(::getpid());
  config.mode = salias::Mode::FifoMpsc;
  config.num_producers = 2;

  auto created = salias::Channel<salias::Mode::FifoMpsc>::create(config);
  if (!created) {
    return 1;
  }

  auto channel = std::move(created).value();
  auto first_publisher = channel.publisher();
  auto second_publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto first = first_publisher.try_claim(sizeof(std::uint32_t));
  if (!first) {
    return 2;
  }
  const std::uint32_t first_value = 1;
  std::memcpy(first->payload().data(), &first_value, sizeof(first_value));

  const std::uint32_t second_value = 2;
  const auto second_bytes = std::as_bytes(std::span{&second_value, std::size_t{1}});
  if (!second_publisher.offer(second_bytes)) {
    return 3;
  }

  auto second_message = subscriber.try_recv();
  if (!second_message || second_message->producer_id != 1) {
    return 4;
  }
  std::uint32_t second_received = 0;
  std::memcpy(&second_received, second_message->payload.data(), sizeof(second_received));
  subscriber.release(*second_message);

  first_publisher.commit(std::move(*first));
  auto first_message = subscriber.try_recv();
  if (!first_message) {
    return 5;
  }
  std::uint32_t first_received = 0;
  std::memcpy(&first_received, first_message->payload.data(), sizeof(first_received));
  subscriber.release(*first_message);

  std::cout << "FIFO received ready producers: " << second_received << ", " << first_received
            << '\n';
  return second_received == second_value && first_received == first_value ? 0 : 6;
}
