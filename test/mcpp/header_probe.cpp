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
  }
  return false;
}
