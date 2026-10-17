#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <source_location>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "core/channel/hybrid_mpsc.hpp"
#include "core/frame/header.hpp"
#include "core/wait/spin_pause.hpp"
#include "salias/salias.hpp"

namespace {
constexpr std::array<std::size_t, 24> lengths{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10,  11,
                                              12, 13, 14, 15, 16, 17, 31, 63, 64, 65, 799, 800};
constexpr std::size_t rounds = 40;
constexpr std::size_t batch_size = 4;

void require(bool condition, std::source_location location = std::source_location::current()) {
  if (!condition) {
    throw std::runtime_error("batch length contract failed at line " +
                             std::to_string(location.line()));
  }
}

std::vector<std::byte> payload_for(std::size_t sequence) {
  const auto length = lengths[(sequence / batch_size) % lengths.size()];
  std::vector<std::byte> payload(length);
  for (std::size_t i = 0; i < length; ++i) {
    payload[i] = static_cast<std::byte>((sequence + i) % 251);
  }
  return payload;
}

template <salias::channel::Order Ordering>
void local_roundtrip() {
  using Channel = salias::channel::HybridMpscChannel<Ordering, salias::wait::SpinPause>;
  auto created = Channel::create(typename Channel::Config{
      .num_producers = 1, .num_consumers = 2, .ring_capacity_per_producer = 4096});
  require(created.has_value());
  auto channel = std::move(*created);
  auto publisher = channel.tx(0);
  std::array subscribers{channel.rx(0), channel.rx(1)};
  for (std::size_t group = 0; group < rounds * lengths.size(); ++group) {
    std::size_t sent = 0;
    while (sent < batch_size) {
      const auto length = static_cast<std::uint32_t>(lengths[group % lengths.size()]);
      auto batch = publisher.claim_batch(length, static_cast<std::uint32_t>(batch_size - sent));
      require(batch && batch->frame_count > 0 && batch->payload_len == length);
      // Poison padding so an oversized receive view cannot accidentally pass comparison.
      std::ranges::fill(batch->region, std::byte{0xa5});
      for (std::size_t index = 0; index < batch->frame_count; ++index) {
        const auto payload = payload_for(group * batch_size + sent + index);
        const auto offset = index * batch->frame_len + salias::frame::kHeaderSize;
        std::ranges::copy(payload, batch->region.subspan(offset, payload.size()).begin());
      }
      publisher.commit_batch(*batch);
      for (auto& subscriber : subscribers) {
        for (std::size_t index = 0; index < batch->frame_count; ++index) {
          const auto message = subscriber.try_recv();
          const auto payload = payload_for(group * batch_size + sent + index);
          require(message && std::ranges::equal(message->payload, payload));
          subscriber.release(*message);
        }
        require(!subscriber.try_recv());
      }
      sent += batch->frame_count;
    }
  }
}

template <salias::Mode M>
void publish_in_child(const std::string& name) {
  auto connected = salias::Channel<M>::connect(name);
  require(connected.has_value());
  auto publisher = connected->publisher();
  for (std::size_t group = 0; group < rounds * lengths.size(); ++group) {
    std::array<std::vector<std::byte>, batch_size> payloads;
    std::array<std::span<const std::byte>, batch_size> views;
    for (std::size_t index = 0; index < batch_size; ++index) {
      payloads[index] = payload_for(group * batch_size + index);
      views[index] = payloads[index];
    }
    std::size_t sent = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (sent < batch_size) {
      const auto offered = publisher.offer_batch(std::span{views}.subspan(sent));
      if (offered) {
        require(*offered > 0);
        sent += *offered;
      } else {
        require(offered.error() == salias::Error::BackPressured);
        require(std::chrono::steady_clock::now() < deadline);
        std::this_thread::yield();
      }
    }
  }
}

template <salias::Mode M>
void ipc_roundtrip() {
  const std::string name =
      "batch-length-" + std::to_string(::getpid()) + "-" + std::to_string(static_cast<int>(M));
  {
    salias::Config config;
    config.name = name;
    config.capacity = 4096;
    constexpr bool fanout = M == salias::Mode::FifoMpmc || M == salias::Mode::OrderedMpmc;
    config.num_consumers = fanout ? 2 : 1;
    auto owner = salias::Channel<M>::create(config);
    require(owner.has_value());
    std::vector<salias::Subscriber<M>> subscribers;
    for (std::size_t i = 0; i < (fanout ? 2u : 1u); ++i) {
      subscribers.push_back(owner->subscriber());
    }
    const auto child = ::fork();
    require(child >= 0);
    if (child == 0) {
      int result = 0;
      try {
        publish_in_child<M>(name);
      } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        result = 1;
      }
      ::_exit(result);
    }
    std::vector<std::size_t> received(subscribers.size());
    bool valid = true;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    const auto total = rounds * lengths.size() * batch_size;
    while (std::ranges::any_of(received, [=](auto count) { return count < total; }) &&
           std::chrono::steady_clock::now() < deadline) {
      for (std::size_t i = 0; i < subscribers.size(); ++i) {
        subscribers[i].poll(7, [&](const salias::Message& message) noexcept {
          const auto payload = payload_for(received[i]);
          valid = valid && std::ranges::equal(message.payload, payload);
          ++received[i];
        });
      }
      std::this_thread::yield();
    }
    int status = 0;
    require(::waitpid(child, &status, 0) == child);
    require(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    require(valid && std::ranges::all_of(received, [=](auto count) { return count == total; }));
  }
  const auto after_cleanup = salias::Channel<M>::connect(name);
  require(!after_cleanup && after_cleanup.error() == salias::Error::NotFound);
}
}  // namespace

int main() {
  try {
    local_roundtrip<salias::channel::Order::Fifo>();
    local_roundtrip<salias::channel::Order::Ordered>();
    ipc_roundtrip<salias::Mode::FifoMpsc>();
    ipc_roundtrip<salias::Mode::FifoMpmc>();
    ipc_roundtrip<salias::Mode::OrderedMpsc>();
    ipc_roundtrip<salias::Mode::OrderedMpmc>();
    std::cout << "Batch lengths, bytes, wraparound, IPC fanout and cleanup passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
