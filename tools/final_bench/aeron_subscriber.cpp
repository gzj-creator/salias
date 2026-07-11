#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "Aeron.h"
#include "concurrent/BusySpinIdleStrategy.h"
#include "tools/final_bench/aeron_common.hpp"

int main(int argc, char** argv) {
  try {
    auto options = salias::final_bench::parse_options(argc, argv);
    salias::final_bench::pin_cpu(options.cpu);
    aeron::Context context;
    context.aeronDir(options.name);
    aeron::Aeron aeron(context);
    const auto registration = aeron.addSubscription(
        salias::final_bench::aeron_channel(options.capacity), 1001);
    auto subscription = salias::final_bench::find_subscription(aeron, registration);
    if (!subscription) return 40;
    while (!subscription->isConnected() || subscription->imageCount() < options.producers) {
      std::this_thread::yield();
    }
    salias::final_bench::touch_file(
        options.coordination_dir / ("subscriber-" + std::to_string(options.index) + ".ready"));
    salias::final_bench::wait_for_file(options.coordination_dir / "start");

    const std::uint64_t expected = options.messages * options.producers;
    std::vector<std::uint64_t> next_sequence(options.producers, 0);
    std::uint64_t consumed = 0;
    std::uint64_t invalid = 0;
    const std::uint64_t start_ns = salias::final_bench::monotonic_now_ns();
    aeron::concurrent::BusySpinIdleStrategy idle;
    auto handler = [&](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                       aeron::util::index_t, aeron::Header&) {
      const std::uint64_t value = static_cast<std::uint64_t>(buffer.getInt64(offset));
      const std::uint32_t producer = static_cast<std::uint32_t>(value >> 48);
      const std::uint64_t sequence = value & ((1ull << 48) - 1);
      if (producer >= next_sequence.size() || sequence != next_sequence[producer]) ++invalid;
      else ++next_sequence[producer];
      ++consumed;
    };
    while (consumed < expected) {
      const int fragments = subscription->poll(handler, static_cast<int>(options.poll_limit));
      if (fragments == 0) idle.idle();
      else idle.reset();
    }
    const std::uint64_t end_ns = salias::final_bench::monotonic_now_ns();
    salias::final_bench::write_result(
        options.coordination_dir / ("subscriber-" + std::to_string(options.index) + ".result"),
        "role=subscriber index=", options.index, " start_ns=", start_ns, " end_ns=", end_ns,
        " consumed=", consumed, " invalid=", invalid);
    return invalid == 0 ? 0 : 41;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
