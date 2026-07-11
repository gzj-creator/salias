#include <cstdint>
#include <cstring>
#include <iostream>
#include <span>
#include <string>
#include <utility>

#include <unistd.h>

#include <salias/salias.hpp>

int main() {
  salias::Config config;
  config.name = "salias-minimal-fifo-" + std::to_string(::getpid());
  config.mode = salias::Mode::FifoMpsc;
  config.num_producers = 2;

  auto created = salias::FifoMpscChannel::create(config);
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

  first->commit();
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
