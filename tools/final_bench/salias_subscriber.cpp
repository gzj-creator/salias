#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "salias/salias.hpp"
#include "tools/final_bench/worker_common.hpp"

namespace {

template <salias::Mode M>
int run(const salias::final_bench::Options& options) {
  using namespace salias::final_bench;
  salias::Config config;
  config.name = options.name;
  config.capacity = options.capacity;
  config.num_producers = options.producers;
  config.num_consumers = options.consumers;

  auto channel_result = options.create ? salias::Channel<M>::create(config)
                                       : salias::Channel<M>::connect(options.name);
  if (!channel_result) return 20;
  auto channel = std::move(channel_result).value();
  auto subscriber = channel.subscriber();
  touch_file(options.coordination_dir / ("subscriber-" + std::to_string(options.index) + ".ready"));
  wait_for_file(options.coordination_dir / "start");

  const std::uint64_t expected = options.messages * options.producers;
  std::vector<std::uint64_t> next_sequence(options.producers, 0);
  std::uint64_t consumed = 0;
  std::uint64_t invalid = 0;
  const std::uint64_t start_ns = monotonic_now_ns();
  while (consumed < expected) {
    const std::size_t got = subscriber.poll(
        options.poll_limit,
        [&](const salias::Message& message) noexcept {
          const std::uint64_t value = read_marker(message.payload);
          const std::uint32_t producer = static_cast<std::uint32_t>(value >> 48);
          const std::uint64_t sequence = value & ((1ull << 48) - 1);
          if (producer >= next_sequence.size() || sequence != next_sequence[producer]) {
            ++invalid;
          } else {
            ++next_sequence[producer];
          }
          ++consumed;
        });
    if (got == 0) std::this_thread::yield();
  }
  const std::uint64_t end_ns = monotonic_now_ns();
  write_result(options.coordination_dir / ("subscriber-" + std::to_string(options.index) + ".result"),
               "role=subscriber index=", options.index, " start_ns=", start_ns,
               " end_ns=", end_ns, " consumed=", consumed, " invalid=", invalid);
  return invalid == 0 ? 0 : 21;
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
