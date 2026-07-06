#pragma once

#include <cstddef>
#include <memory>
#include <span>

#include "salias/error.hpp"

namespace salias {

class Channel;
struct ChannelState;

class Publisher {
 public:
  Result<bool> offer(std::span<const std::byte> payload) noexcept;

 private:
  explicit Publisher(std::shared_ptr<ChannelState> state) noexcept;

  std::shared_ptr<ChannelState> state_;

  friend class Channel;
};

}  // namespace salias
