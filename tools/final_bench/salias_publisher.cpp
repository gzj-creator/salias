#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <string>
#include <vector>

#include "salias/salias.hpp"
#include "tools/final_bench/worker_common.hpp"

namespace {

template <salias::Mode M>
int run(const salias::final_bench::Options& options) {
  using namespace salias::final_bench;
  auto connected = salias::Channel<M>::connect(options.name);
  if (!connected) return 10;
  auto channel = std::move(connected).value();
  auto publisher = channel.publisher();
  touch_file(options.coordination_dir / ("publisher-" + std::to_string(options.index) + ".ready"));
  wait_for_file(options.coordination_dir / "start");

  std::uint64_t backpressured = 0;
  std::uint64_t attempts = 0;
  std::uint64_t max_consecutive_retries = 0;
  std::uint64_t consecutive_retries = 0;
  std::uint64_t published = 0;
  const std::uint64_t start_ns = monotonic_now_ns();

  if (options.aligned && options.batch_size == 1) {
    while (published < options.messages) {
      ++attempts;
      auto claimed = publisher.try_claim(kPayloadSize);
      if (claimed) {
        write_payload(claimed->payload(), options.index, published);
        claimed->commit();
        ++published;
        consecutive_retries = 0;
      } else if (claimed.error() == salias::Error::BackPressured) {
        ++backpressured;
        ++consecutive_retries;
        max_consecutive_retries = std::max(max_consecutive_retries, consecutive_retries);
        std::this_thread::yield();
      } else {
        return 13;
      }
    }
  } else if (options.batch_size == 1) {
    std::array<std::byte, kPayloadSize> payload{};
    while (published < options.messages) {
      write_payload(payload, options.index, published);
      ++attempts;
      auto offered = publisher.offer(payload);
      if (offered && offered.value()) {
        ++published;
        consecutive_retries = 0;
      } else if (!offered && offered.error() == salias::Error::BackPressured) {
        ++backpressured;
        ++consecutive_retries;
        max_consecutive_retries = std::max(max_consecutive_retries, consecutive_retries);
        std::this_thread::yield();
      } else {
        return 11;
      }
    }
  } else {
    std::vector<std::array<std::byte, kPayloadSize>> storage(options.batch_size);
    std::vector<std::span<const std::byte>> payloads(options.batch_size);
    while (published < options.messages) {
      const std::size_t count = static_cast<std::size_t>(
          std::min<std::uint64_t>(options.batch_size, options.messages - published));
      for (std::size_t i = 0; i < count; ++i) {
        write_payload(storage[i], options.index, published + i);
        payloads[i] = storage[i];
      }
      ++attempts;
      auto offered = publisher.offer_batch(std::span(payloads.data(), count));
      if (offered && offered.value() > 0) {
        published += offered.value();
        consecutive_retries = 0;
      } else if (!offered && offered.error() == salias::Error::BackPressured) {
        ++backpressured;
        ++consecutive_retries;
        max_consecutive_retries = std::max(max_consecutive_retries, consecutive_retries);
        std::this_thread::yield();
      } else {
        return 12;
      }
    }
  }

  const std::uint64_t end_ns = monotonic_now_ns();
  write_result(options.coordination_dir / ("publisher-" + std::to_string(options.index) + ".result"),
               "role=publisher index=", options.index, " start_ns=", start_ns,
               " end_ns=", end_ns, " published=", published, " attempts=", attempts,
               " backpressured=", backpressured,
               " max_consecutive_retries=", max_consecutive_retries);
  return 0;
}

int dispatch(const salias::final_bench::Options& options) {
  const bool fanout = options.consumers > 1;
  if (options.mode == "fifo") {
    return fanout ? run<salias::Mode::FifoFanout>(options) : run<salias::Mode::FifoMpsc>(options);
  }
  if (options.mode == "ordered") {
    return fanout ? run<salias::Mode::OrderedFanout>(options)
                  : run<salias::Mode::OrderedMpsc>(options);
  }
  return 2;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    auto options = salias::final_bench::parse_options(argc, argv);
    salias::final_bench::pin_cpu(options.cpu);
    return dispatch(options);
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
