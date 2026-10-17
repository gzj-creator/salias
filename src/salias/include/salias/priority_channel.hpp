/**
 * @file src/salias/include/salias/priority_channel.hpp
 * @brief Route publications and fairly coordinate reads across independent IPC channels.
 */
#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <span>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>

#include "salias/config.hpp"
#include "salias/error.hpp"
#include "salias/message.hpp"
#include "salias/publisher.hpp"
#include "salias/subscriber.hpp"

namespace salias {

namespace priority_detail {

template <std::size_t Index, Mode First, Mode... Rest>
struct ModeAt : ModeAt<Index - 1, Rest...> {};

template <Mode First, Mode... Rest>
struct ModeAt<0, First, Rest...> : std::integral_constant<Mode, First> {};

}  // namespace priority_detail

/// @brief Borrowed message tagged with its zero-based priority stream index.
struct PriorityMessage {
  std::uint32_t priority = 0;
  Message message{};
};

/**
 * @brief Publisher router for independently backpressured priority channels.
 * @tparam Modes One channel mode per priority, ordered highest to lowest.
 *
 * Each offer targets exactly one underlying channel. A full or busy stream does not consume
 * capacity or change the result of offers to other streams. Use publisher<Priority>() to access
 * the full underlying Publisher API, including zero-copy claims.
 */
template <Mode... Modes>
class PriorityPublisher {
  static constexpr std::size_t kPriorityCount = sizeof...(Modes);
  static_assert(kPriorityCount >= 2, "PriorityPublisher requires at least two channels");

 public:
  PriorityPublisher() noexcept = default;

  explicit PriorityPublisher(Publisher<Modes>... publishers) noexcept
      : publishers_(std::in_place, std::move(publishers)...), valid_(true) {}

  PriorityPublisher(const PriorityPublisher&) = default;
  PriorityPublisher& operator=(const PriorityPublisher&) = default;
  PriorityPublisher(PriorityPublisher&& other) noexcept
      : publishers_(std::move(other.publishers_)), valid_(std::exchange(other.valid_, false)) {}
  PriorityPublisher& operator=(PriorityPublisher&& other) noexcept {
    if (this != &other) {
      publishers_ = std::move(other.publishers_);
      valid_ = std::exchange(other.valid_, false);
    }
    return *this;
  }

  /// @brief Offer to one priority, where index zero is the highest priority.
  Result<bool> offer(std::size_t priority, std::span<const std::byte> payload) noexcept {
    if (!valid_ || priority >= kPriorityCount) {
      return std::unexpected(Error::BadConfig);
    }
    return offer_at(priority, payload);
  }

  /// @brief Compile-time routed offer with no runtime priority lookup.
  template <std::size_t Priority>
  Result<bool> offer(std::span<const std::byte> payload) noexcept {
    static_assert(Priority < kPriorityCount, "Priority index is out of range");
    if (!valid_) {
      return std::unexpected(Error::BadConfig);
    }
    return std::get<Priority>(*publishers_).offer(payload);
  }

  /// @brief Offer with a TTL to one priority stream.
  Result<bool> offer_for(std::size_t priority, std::span<const std::byte> payload,
                         std::chrono::nanoseconds ttl) noexcept {
    if (!valid_ || priority >= kPriorityCount) {
      return std::unexpected(Error::BadConfig);
    }
    return offer_for_at(priority, payload, ttl);
  }

  /// @brief Offer a batch to one priority stream.
  Result<std::size_t> offer_batch(std::size_t priority,
                                  std::span<const std::span<const std::byte>> payloads) noexcept {
    if (!valid_ || priority >= kPriorityCount) {
      return std::unexpected(Error::BadConfig);
    }
    return offer_batch_at(priority, payloads);
  }

  /// @brief Access an underlying publisher for claims or other mode-specific operations.
  template <std::size_t Priority>
  Result<std::reference_wrapper<Publisher<priority_detail::ModeAt<Priority, Modes...>::value>>>
  publisher() noexcept {
    static_assert(Priority < kPriorityCount, "Priority index is out of range");
    if (!valid_) {
      return std::unexpected(Error::BadConfig);
    }
    return std::ref(std::get<Priority>(*publishers_));
  }

  explicit operator bool() const noexcept { return valid_; }
  static constexpr std::size_t priority_count() noexcept { return kPriorityCount; }

 private:
  template <std::size_t Index = 0>
  Result<bool> offer_at(std::size_t priority, std::span<const std::byte> payload) noexcept {
    if constexpr (Index == kPriorityCount) {
      return std::unexpected(Error::BadConfig);
    } else if (priority == Index) {
      return std::get<Index>(*publishers_).offer(payload);
    } else {
      return offer_at<Index + 1>(priority, payload);
    }
  }

  template <std::size_t Index = 0>
  Result<bool> offer_for_at(std::size_t priority, std::span<const std::byte> payload,
                            std::chrono::nanoseconds ttl) noexcept {
    if constexpr (Index == kPriorityCount) {
      return std::unexpected(Error::BadConfig);
    } else if (priority == Index) {
      return std::get<Index>(*publishers_).offer_for(payload, ttl);
    } else {
      return offer_for_at<Index + 1>(priority, payload, ttl);
    }
  }

  template <std::size_t Index = 0>
  Result<std::size_t> offer_batch_at(
      std::size_t priority, std::span<const std::span<const std::byte>> payloads) noexcept {
    if constexpr (Index == kPriorityCount) {
      return std::unexpected(Error::BadConfig);
    } else if (priority == Index) {
      return std::get<Index>(*publishers_).offer_batch(payloads);
    } else {
      return offer_batch_at<Index + 1>(priority, payloads);
    }
  }

  std::optional<std::tuple<Publisher<Modes>...>> publishers_;
  bool valid_ = false;
};

/**
 * @brief Fair, priority-aware reader across independent channel subscribers.
 * @tparam Modes One channel mode per priority, ordered highest to lowest.
 *
 * Scheduling is work-conserving weighted round robin. With N streams, stream i receives a
 * consecutive-service budget of N-i messages; priority zero is favored, while every continuously
 * readable lower-priority stream is still selected within a finite number of deliveries. FIFO
 * order is preserved within each stream, but no total order is promised across streams. A
 * BackPressured stream is skipped for that poll attempt so another stream can make progress.
 * Returned messages borrow their source channel and must be released through this reader before
 * another message from that stream is requested. A Lagged result from an underlying LatestMpmc
 * is treated as an empty attempt; inspect subscriber<Priority>().lagged_messages() for loss.
 * This object is move-only and, like its underlying subscribers, is not safe for concurrent reads.
 * Do not consume through subscriber<Priority>(); the accessor is intended for read-only counters.
 */
template <Mode... Modes>
class PrioritySubscriber {
  static constexpr std::size_t kPriorityCount = sizeof...(Modes);
  static_assert(kPriorityCount >= 2, "PrioritySubscriber requires at least two channels");
  static_assert(kPriorityCount <= std::numeric_limits<std::uint32_t>::max(),
                "PrioritySubscriber has too many channels");

 public:
  PrioritySubscriber() noexcept = default;

  explicit PrioritySubscriber(Subscriber<Modes>... subscribers) noexcept
      : subscribers_(std::move(subscribers)...), valid_(true) {}

  PrioritySubscriber(const PrioritySubscriber&) = delete;
  PrioritySubscriber& operator=(const PrioritySubscriber&) = delete;
  PrioritySubscriber(PrioritySubscriber&& other) noexcept
      : subscribers_(std::move(other.subscribers_)),
        outstanding_(other.outstanding_),
        next_priority_(other.next_priority_),
        remaining_credit_(other.remaining_credit_),
        valid_(std::exchange(other.valid_, false)) {}
  PrioritySubscriber& operator=(PrioritySubscriber&& other) noexcept {
    if (this != &other) {
      subscribers_ = std::move(other.subscribers_);
      outstanding_ = other.outstanding_;
      next_priority_ = other.next_priority_;
      remaining_credit_ = other.remaining_credit_;
      valid_ = std::exchange(other.valid_, false);
    }
    return *this;
  }

  /**
   * @brief Read the next message according to weighted priority scheduling.
   * @return A priority-tagged borrowed message, empty when all streams are empty, or a surfaced
   *         non-contention error from an underlying subscriber.
   * @retval Error::BackPressured All streams were empty or temporarily contended and at least one
   *         was contended.
   * @retval Error::BadConfig This is a default or moved-from reader, or an underlying endpoint is
   *         invalid.
   */
  Result<std::optional<PriorityMessage>> try_recv_checked() noexcept {
    if (!valid_) {
      return std::unexpected(Error::BadConfig);
    }

    bool saw_backpressure = false;
    for (std::size_t attempt = 0; attempt < kPriorityCount; ++attempt) {
      auto received = receive_at(next_priority_, saw_backpressure);
      if (!received) {
        return std::unexpected(received.error());
      }
      if (*received) {
        if (remaining_credit_ > 1) {
          --remaining_credit_;
        } else {
          advance_priority();
        }
        return received;
      }
      advance_priority();
    }
    if (saw_backpressure) {
      return std::unexpected(Error::BackPressured);
    }
    return std::optional<PriorityMessage>{};
  }

  /// @brief Convenience read; contention and other errors appear as temporarily empty.
  std::optional<PriorityMessage> try_recv() noexcept {
    auto received = try_recv_checked();
    return received ? std::move(*received) : std::nullopt;
  }

  /** @brief Block until one stream produces a message; non-contention errors are returned. */
  Result<PriorityMessage> recv() noexcept {
    for (;;) {
      auto received = try_recv_checked();
      if (!received) {
        if (received.error() != Error::BackPressured) {
          return std::unexpected(received.error());
        }
      } else if (*received) {
        return std::move(**received);
      }
      std::this_thread::yield();
    }
  }

  /** @brief Release a message returned by this reader to its source channel. */
  Result<bool> release(const PriorityMessage& message) noexcept {
    if (!valid_ || message.priority >= kPriorityCount || !outstanding_[message.priority]) {
      return std::unexpected(Error::BadConfig);
    }
    auto released = release_at(message.priority, message.message);
    if (released) {
      outstanding_[message.priority] = false;
    }
    return released;
  }

  /** @brief Poll and release up to max_messages after each noexcept callback. */
  template <class Handler>
  std::size_t poll(std::uint32_t max_messages, Handler&& handler) noexcept {
    using HandlerType = std::remove_reference_t<Handler>;
    static_assert(
        std::is_nothrow_invocable_v<HandlerType&, const PriorityMessage&>,
        "PrioritySubscriber::poll handler must be noexcept and accept a priority message");
    std::size_t consumed = 0;
    while (consumed < max_messages) {
      auto received = try_recv_checked();
      if (!received || !*received) {
        break;
      }
      handler(**received);
      static_cast<void>(release(**received));
      ++consumed;
    }
    return consumed;
  }

  /// @brief Access one underlying subscriber for read-only loss and expiry counters.
  template <std::size_t Priority>
  const Subscriber<priority_detail::ModeAt<Priority, Modes...>::value>& subscriber()
      const noexcept {
    static_assert(Priority < kPriorityCount, "Priority index is out of range");
    return std::get<Priority>(subscribers_);
  }

  explicit operator bool() const noexcept { return valid_; }
  static constexpr std::size_t priority_count() noexcept { return kPriorityCount; }

 private:
  void advance_priority() noexcept {
    next_priority_ = (next_priority_ + 1) % kPriorityCount;
    remaining_credit_ = kPriorityCount - next_priority_;
  }

  template <std::size_t Index = 0>
  Result<std::optional<PriorityMessage>> receive_at(std::size_t priority,
                                                    bool& saw_backpressure) noexcept {
    if constexpr (Index == kPriorityCount) {
      return std::unexpected(Error::BadConfig);
    } else if (priority == Index) {
      if (outstanding_[Index]) {
        return std::optional<PriorityMessage>{};
      }
      auto received = std::get<Index>(subscribers_).try_recv_checked();
      if (!received) {
        if (received.error() == Error::BackPressured) {
          saw_backpressure = true;
          return std::optional<PriorityMessage>{};
        }
        if (received.error() == Error::Lagged) {
          return std::optional<PriorityMessage>{};
        }
        return std::unexpected(received.error());
      }
      if (!*received) {
        return std::optional<PriorityMessage>{};
      }
      outstanding_[Index] = true;
      return std::optional<PriorityMessage>{
          PriorityMessage{static_cast<std::uint32_t>(Index), std::move(**received)}};
    } else {
      return receive_at<Index + 1>(priority, saw_backpressure);
    }
  }

  template <std::size_t Index = 0>
  Result<bool> release_at(std::size_t priority, const Message& message) noexcept {
    if constexpr (Index == kPriorityCount) {
      return std::unexpected(Error::BadConfig);
    } else if (priority == Index) {
      std::get<Index>(subscribers_).release(message);
      return true;
    } else {
      return release_at<Index + 1>(priority, message);
    }
  }

  std::tuple<Subscriber<Modes>...> subscribers_{};
  std::array<bool, kPriorityCount> outstanding_{};
  std::size_t next_priority_ = 0;
  std::size_t remaining_credit_ = kPriorityCount;
  bool valid_ = false;
};

}  // namespace salias
