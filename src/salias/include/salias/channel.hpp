#pragma once

#include <memory>

#include "salias/config.hpp"
#include "salias/error.hpp"
#include "salias/publisher.hpp"
#include "salias/subscriber.hpp"

namespace salias {

struct ChannelState;

class Channel {
 public:
  static Result<Channel> create(const Config& config);

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
