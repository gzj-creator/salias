#pragma once

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
  explicit Subscriber(std::shared_ptr<ChannelState> state) noexcept;

  std::shared_ptr<ChannelState> state_;

  friend class Channel;
};

}  // namespace salias
