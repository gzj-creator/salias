#pragma once

#include <optional>
#include <span>
#include <utility>

#include "core/channel/channel_config.hpp"
#include "core/channel/error.hpp"
#include "core/channel/spsc.hpp"
#include "core/flow/consumer.hpp"
#include "core/flow/error.hpp"
#include "core/flow/producer.hpp"
#include "core/platform/result.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

template <wait::WaitStrategy Wait = wait::SpinPause>
class BulkChannel {
 public:
  using CreateResult = platform::Result<BulkChannel, ChannelError>;
  using OfferResult = platform::Result<bool, flow::FlowError>;

  class Tx;
  class Rx;

  static CreateResult create(const ChannelConfig& config) noexcept {
    if (config.fixed_size) {
      return CreateResult::failure(ChannelError::BadConfig);
    }

    auto channel = SpscChannel<Wait>::create(config);
    if (!channel) {
      return CreateResult::failure(channel.error());
    }

    return CreateResult::success(BulkChannel(std::move(channel).value()));
  }

  BulkChannel(BulkChannel&&) noexcept = default;
  BulkChannel& operator=(BulkChannel&&) noexcept = default;
  BulkChannel(const BulkChannel&) = delete;
  BulkChannel& operator=(const BulkChannel&) = delete;

  std::size_t capacity() const noexcept { return channel_.capacity(); }

  Tx tx() noexcept { return Tx(channel_.tx()); }
  Rx rx() noexcept { return Rx(channel_.rx()); }

 private:
  explicit BulkChannel(SpscChannel<Wait> channel) noexcept : channel_(std::move(channel)) {}

  SpscChannel<Wait> channel_;

 public:
  // Bulk producer endpoint. It intentionally preserves the SPSC claim/commit contract: a message
  // must fit in one frame, otherwise L3 returns MessageTooLarge and no fragmentation path exists.
  class Tx {
   public:
    explicit Tx(typename SpscChannel<Wait>::Tx tx) noexcept : tx_(std::move(tx)) {}

    flow::Producer::ClaimResult claim(std::uint32_t len) noexcept { return tx_.claim(len); }
    void commit(const flow::Claim& claim) noexcept { tx_.commit(claim); }
    OfferResult offer(std::span<const std::byte> payload) noexcept { return tx_.offer(payload); }

   private:
    typename SpscChannel<Wait>::Tx tx_;
  };

  // Bulk consumer endpoint. The returned payload span is contiguous for the full message because
  // L0 maps the ring twice into adjacent virtual memory.
  class Rx {
   public:
    explicit Rx(typename SpscChannel<Wait>::Rx rx) noexcept : rx_(std::move(rx)) {}

    std::optional<flow::Message> try_recv() noexcept { return rx_.try_recv(); }
    flow::Message recv() noexcept { return rx_.recv(); }
    void release(const flow::Message& message) noexcept { rx_.release(message); }

   private:
    typename SpscChannel<Wait>::Rx rx_;
  };
};

}  // namespace salias::channel
