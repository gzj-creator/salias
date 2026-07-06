#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "salias/message.hpp"

namespace salias {

class Channel;
struct ChannelState;

class Subscriber {
 public:
  std::optional<Message> try_recv() noexcept;
  Message recv() noexcept;
  void release(const Message& message) noexcept;

 private:
  explicit Subscriber(std::shared_ptr<ChannelState> state,
                      std::uint32_t subscription_index = 0) noexcept;

  std::shared_ptr<ChannelState> state_;
  std::uint32_t subscription_index_ = 0;

  friend class Channel;
};

}  // namespace salias
