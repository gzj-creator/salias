#pragma once

#include <memory>
#include <string_view>

#include "salias/config.hpp"
#include "salias/error.hpp"
#include "salias/publisher.hpp"
#include "salias/subscriber.hpp"

namespace salias {

struct ChannelState;

class Channel {
 public:
  // Creates an in-process channel when Config::name is empty. For named SPSC channels, this is the
  // owner endpoint: it creates the shared memory control/ring objects and publishes ready metadata.
  static Result<Channel> create(const Config& config);

  // Connects to an existing or soon-to-exist named SPSC channel. The call waits boundedly for the
  // owner to publish its ready flag, then validates the shared metadata before mapping the ring.
  static Result<Channel> connect(std::string_view name);

  Channel(Channel&&) noexcept = default;
  Channel& operator=(Channel&&) noexcept = default;
  Channel(const Channel&) = default;
  Channel& operator=(const Channel&) = default;

  Publisher publisher() noexcept;
  Subscriber subscriber() noexcept;

 private:
  explicit Channel(std::shared_ptr<ChannelState> state) noexcept;

  std::shared_ptr<ChannelState> state_;
};

}  // namespace salias
