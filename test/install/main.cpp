#include <unistd.h>

#include <cstdint>
#include <salias/salias.hpp>
#include <string>

namespace {

struct InstallFixedMessage {
  std::uint64_t sequence;
};

struct InstallFixedSchema {
  using message_type = InstallFixedMessage;
  static constexpr std::uint64_t schema_id = 0x494E5354414C4C31ull;
  static constexpr std::uint32_t version = 1;
};

}  // namespace

int main() {
  salias::Config config;
  config.name = "install-consumer";
  salias::PrioritySubscriber<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc> reader;
  for (const auto policy : {salias::PriorityWaitPolicy::Yield, salias::PriorityWaitPolicy::Spin,
                            salias::PriorityWaitPolicy::Adaptive}) {
    auto result = reader.recv(policy);
    if (result || result.error() != salias::Error::BadConfig) {
      return 2;
    }
  }
  if (config.capacity == 0) {
    return 1;
  }

  using FixedChannel = salias::FixedChannel<salias::FixedTopology::Spsc, InstallFixedSchema, 64>;
  auto channel = FixedChannel::create("install-consumer-fixed-" + std::to_string(::getpid()));
  if (!channel) {
    return 3;
  }
  auto publisher = channel->publisher(0);
  auto subscriber = channel->subscriber(0);
  if (!publisher || !subscriber) {
    return 4;
  }
  if (!(*publisher).send(InstallFixedMessage{.sequence = 42})) {
    return 5;
  }
  auto received = (*subscriber).try_recv_copy();
  if (!received || !*received || (**received).sequence != 42) {
    return 6;
  }

  using LatestChannel = salias::LatestMpmcChannel<4096, 75, InstallFixedSchema>;
  salias::Config latest_config;
  latest_config.name = "install-consumer-latest-" + std::to_string(::getpid());
  auto latest = LatestChannel::create(latest_config);
  if (!latest || latest->capacity() != 4096 || latest->high_watermark_percent() != 75) {
    return 7;
  }
  auto latest_publisher = latest->publisher();
  auto latest_subscriber = latest->subscriber();
  if (!latest_publisher.send(InstallFixedMessage{.sequence = 43})) {
    return 8;
  }
  auto latest_received = latest_subscriber.try_recv_copy();
  if (!latest_received || !*latest_received || (**latest_received).sequence != 43) {
    return 9;
  }
  return 0;
}
