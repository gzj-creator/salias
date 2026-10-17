#include <array>
#include <salias/salias.hpp>

template <salias::Mode M>
bool publish_from_header(const salias::Config& config) {
  auto connected = salias::Channel<M>::connect(config.name);
  if (!connected) {
    return false;
  }
  auto publisher = connected->publisher();
  const std::array payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
  const auto offered = publisher.offer(payload);
  return offered && *offered;
}

bool header_publish(const salias::Config& config) {
  switch (config.mode) {
    case salias::Mode::FifoMpsc:
      return publish_from_header<salias::Mode::FifoMpsc>(config);
    case salias::Mode::FifoFanout:
      return publish_from_header<salias::Mode::FifoFanout>(config);
    case salias::Mode::OrderedMpsc:
      return publish_from_header<salias::Mode::OrderedMpsc>(config);
    case salias::Mode::OrderedFanout:
      return publish_from_header<salias::Mode::OrderedFanout>(config);
    case salias::Mode::LatestFanout:
      return publish_from_header<salias::Mode::LatestFanout>(config);
  }
  return false;
}

bool header_keyed_latest_publish(std::string_view name) {
  using Keyed = salias::KeyedLatestChannel<std::uint64_t, std::uint64_t, 4>;
  auto connected = Keyed::connect(name);
  if (!connected) {
    return false;
  }
  auto publisher = connected->publisher();
  auto offered = publisher.offer(7, 12);
  return offered && *offered;
}

bool header_priority_publish(std::string_view high_name, std::string_view low_name) {
  auto high = salias::FifoMpscChannel::connect(high_name);
  auto low = salias::FifoMpscChannel::connect(low_name);
  if (!high || !low) {
    return false;
  }
  salias::PriorityPublisher<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc> publisher(
      high->publisher(), low->publisher());
  const std::array payload{std::byte{9}, std::byte{8}};
  auto offered = publisher.offer(1, payload);
  return offered && *offered;
}
