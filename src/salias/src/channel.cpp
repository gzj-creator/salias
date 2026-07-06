#include "salias/channel.hpp"

#include <memory>
#include <optional>
#include <utility>

#include "core/channel/spsc.hpp"

namespace salias {

namespace {

Error map_flow_error(flow::FlowError error) noexcept {
  switch (error) {
    case flow::FlowError::Ok:
      return Error::Ok;
    case flow::FlowError::BackPressured:
      return Error::BackPressured;
    case flow::FlowError::MessageTooLarge:
      return Error::MessageTooLarge;
  }
  return Error::BadConfig;
}

}  // namespace

struct ChannelState {
  explicit ChannelState(channel::SpscChannel<> channel_value) noexcept
      : channel(std::move(channel_value)) {}

  channel::SpscChannel<> channel;
};

Channel::Channel(std::shared_ptr<ChannelState> state) noexcept : state_(std::move(state)) {}

Result<Channel> Channel::create(const Config& config) {
  if (!config.name.empty() || config.mode != Mode::Spsc || config.fixed_size ||
      config.record_size != 0 || config.wait != WaitKind::SpinPause) {
    return Result<Channel>::failure(Error::BadConfig);
  }

  auto created = channel::SpscChannel<>::create(channel::ChannelConfig{.capacity = config.capacity});
  if (!created) {
    return Result<Channel>::failure(created.error() == channel::ChannelError::BadConfig
                                        ? Error::BadConfig
                                        : Error::PlatformFail);
  }

  return Result<Channel>::success(
      Channel(std::make_shared<ChannelState>(std::move(created).value())));
}

Publisher Channel::publisher() noexcept {
  return Publisher(state_);
}

Subscriber Channel::subscriber() noexcept {
  return Subscriber(state_);
}

Publisher::Publisher(std::shared_ptr<ChannelState> state) noexcept : state_(std::move(state)) {}

Result<bool> Publisher::offer(std::span<const std::byte> payload) noexcept {
  auto tx = state_->channel.tx();
  auto offered = tx.offer(payload);
  if (!offered) {
    return Result<bool>::failure(map_flow_error(offered.error()));
  }
  return Result<bool>::success(offered.value());
}

Subscriber::Subscriber(std::shared_ptr<ChannelState> state) noexcept : state_(std::move(state)) {}

std::optional<Message> Subscriber::try_recv() noexcept {
  auto rx = state_->channel.rx();
  auto message = rx.try_recv();
  if (!message) {
    return std::nullopt;
  }
  return Message{
      .payload = message->payload,
      .position = message->position,
      .next_position = message->next_position,
      .meta = message->meta,
  };
}

Message Subscriber::recv() noexcept {
  auto rx = state_->channel.rx();
  auto message = rx.recv();
  return Message{
      .payload = message.payload,
      .position = message.position,
      .next_position = message.next_position,
      .meta = message.meta,
  };
}

void Subscriber::release(const Message& message) noexcept {
  auto rx = state_->channel.rx();
  rx.release(flow::Message{
      .payload = message.payload,
      .position = message.position,
      .next_position = message.next_position,
      .meta = message.meta,
  });
}

}  // namespace salias
