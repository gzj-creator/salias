#include <salias/channel.hpp>

#include <cstdint>
#include <cstring>
#include <string>
#include <utility>

#include <unistd.h>

namespace {

struct Payload {
  std::uint64_t sequence = 0;
};

void write_payload(salias::PublishClaim<salias::Mode::FifoMpsc>& claim,
                   std::uint64_t sequence) {
  const Payload payload{.sequence = sequence};
  std::memcpy(claim.payload().data(), &payload, sizeof(payload));
}

}  // namespace

int main() {
  salias::Config config;
  config.name = "commit-visibility-contract-" + std::to_string(::getpid());
  config.capacity = 1u << 20;

  auto created = salias::FifoMpscChannel::create(config);
  if (!created) {
    return 1;
  }
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto first = publisher.try_claim(sizeof(Payload));
  auto second = publisher.try_claim(sizeof(Payload));
  if (!first || !second) {
    return 2;
  }
  write_payload(first.value(), 0);
  write_payload(second.value(), 1);

  second->commit();
  if (subscriber.try_recv()) {
    return 3;
  }
  first->commit();

  for (std::uint64_t expected = 0; expected < 2; ++expected) {
    auto message = subscriber.try_recv();
    if (!message) {
      return 4;
    }
    Payload received{};
    std::memcpy(&received, message->payload.data(), sizeof(received));
    if (received.sequence != expected) {
      return 5;
    }
    subscriber.release(*message);
  }
}
