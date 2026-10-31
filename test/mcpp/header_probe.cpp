#include <array>
#include <salias/salias.hpp>

#include "typed_schema.hpp"

template <salias::Mode M>
bool publish_from_header(const salias::Config& config) {
  auto connected = salias::Channel<M>::connect(config.name);
  if (!connected) {
    return false;
  }
  auto publisher = connected->publisher();
  const std::array payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
  const auto offered = publisher.send(payload);
  return offered && *offered;
}

bool publish_latest_from_header(const salias::Config& config) {
  using Channel = salias::LatestMpmcChannel<4096, 75>;
  auto connected = Channel::connect(config.name);
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
    case salias::Mode::FifoMpmc:
      return publish_from_header<salias::Mode::FifoMpmc>(config);
    case salias::Mode::OrderedMpsc:
      return publish_from_header<salias::Mode::OrderedMpsc>(config);
    case salias::Mode::OrderedMpmc:
      return publish_from_header<salias::Mode::OrderedMpmc>(config);
    case salias::Mode::LatestMpmc:
      return publish_latest_from_header(config);
  }
  return false;
}

bool header_typed_publish(std::string_view name, const salias_mcpp::TypedPayload& payload) {
  using Channel = salias::Channel<salias::Mode::FifoMpsc, salias_mcpp::TypedPayloadSchema>;
  auto connected = Channel::connect(name);
  if (!connected) {
    return false;
  }
  auto publisher = connected->publisher();
  auto offered = publisher.send(payload);
  return offered && *offered;
}
