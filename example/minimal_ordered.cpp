#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <utility>

#include <unistd.h>

#include <salias/salias.hpp>

int main() {
  salias::Config config;
  config.name = "salias-minimal-ordered-" + std::to_string(::getpid());
  config.mode = salias::Mode::OrderedMpsc;
  config.num_producers = 2;

  auto created = salias::OrderedMpscChannel::create(config);
  if (!created) {
    return 1;
  }

  auto channel = std::move(created).value();
  auto first_publisher = channel.publisher();
  auto second_publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto first = first_publisher.try_claim(sizeof(std::uint32_t));
  auto second = second_publisher.try_claim(sizeof(std::uint32_t));
  if (!first || !second) {
    return 2;
  }

  const std::uint32_t first_value = 1;
  const std::uint32_t second_value = 2;
  std::memcpy(first->payload().data(), &first_value, sizeof(first_value));
  std::memcpy(second->payload().data(), &second_value, sizeof(second_value));

  second->commit();
  if (subscriber.try_recv()) {
    return 3;
  }

  first->commit();
  for (const std::uint32_t expected : {first_value, second_value}) {
    auto message = subscriber.try_recv();
    if (!message) {
      return 4;
    }
    std::uint32_t received = 0;
    std::memcpy(&received, message->payload.data(), sizeof(received));
    subscriber.release(*message);
    if (received != expected) {
      return 5;
    }
  }

  std::cout << "Ordered received global sequence: 1, 2\n";
  return 0;
}
