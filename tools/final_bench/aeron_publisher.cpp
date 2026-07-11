#include <algorithm>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>

#include "Aeron.h"
#include "concurrent/BusySpinIdleStrategy.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "tools/final_bench/aeron_common.hpp"

int main(int argc, char** argv) {
  try {
    auto options = salias::final_bench::parse_options(argc, argv);
    salias::final_bench::pin_cpu(options.cpu);
    aeron::Context context;
    context.aeronDir(options.name);
    aeron::Aeron aeron(context);
    const auto registration = aeron.addExclusivePublication(
        salias::final_bench::aeron_channel(options.capacity), 1001);
    auto publication = salias::final_bench::find_publication(aeron, registration);
    if (!publication) return 30;
    while (!publication->isConnected()) std::this_thread::yield();
    salias::final_bench::touch_file(
        options.coordination_dir / ("publisher-" + std::to_string(options.index) + ".ready"));
    salias::final_bench::wait_for_file(options.coordination_dir / "start");

    aeron::concurrent::BusySpinIdleStrategy idle;
    aeron::concurrent::logbuffer::BufferClaim claim;
    std::uint64_t backpressured = 0;
    std::uint64_t attempts = 0;
    std::uint64_t max_consecutive_retries = 0;
    std::uint64_t consecutive_retries = 0;
    const std::uint64_t start_ns = salias::final_bench::monotonic_now_ns();
    for (std::uint64_t sequence = 0; sequence < options.messages;) {
      ++attempts;
      const auto position = publication->tryClaim(salias::final_bench::kPayloadSize, claim);
      if (position > 0) {
        claim.buffer().putInt64(
            claim.offset(),
            static_cast<std::int64_t>(salias::final_bench::marker(options.index, sequence)));
        claim.commit();
        ++sequence;
        consecutive_retries = 0;
        idle.reset();
        continue;
      }
      if (position == aeron::PUBLICATION_CLOSED || position == aeron::MAX_POSITION_EXCEEDED) {
        return 31;
      }
      ++backpressured;
      ++consecutive_retries;
      max_consecutive_retries = std::max(max_consecutive_retries, consecutive_retries);
      idle.idle();
    }
    const std::uint64_t end_ns = salias::final_bench::monotonic_now_ns();
    salias::final_bench::write_result(
        options.coordination_dir / ("publisher-" + std::to_string(options.index) + ".result"),
        "role=publisher index=", options.index, " start_ns=", start_ns, " end_ns=", end_ns,
        " published=", options.messages, " attempts=", attempts,
        " backpressured=", backpressured,
        " max_consecutive_retries=", max_consecutive_retries);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
