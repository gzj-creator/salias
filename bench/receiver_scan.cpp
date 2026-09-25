#include <salias/salias.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>

namespace {
using Clock = std::chrono::steady_clock;
constexpr std::uint64_t kMessages = 4096;
constexpr int kRounds = 32;
struct Payload {
  std::uint64_t sequence;
  std::uint64_t padding[7]{};
};

template <salias::Mode M, bool Batch>
double measure(std::uint32_t producers, bool sparse) {
  salias::Config config;
  config.name = "receiver-scan-" + std::to_string(::getpid());
  config.capacity = 1u << 20;
  config.num_producers = producers;
  auto created = salias::Channel<M>::create(config);
  if (!created) {
    std::cerr << "channel creation failed" << std::endl;
    std::exit(1);
  }
  auto channel = std::move(created).value();
  std::vector<salias::Publisher<M>> publishers;
  for (std::uint32_t p = 0; p < producers; ++p) {
    publishers.push_back(channel.publisher());
  }
  auto subscriber = channel.subscriber();
  const std::uint64_t active = sparse ? 1 : producers;
  const std::uint64_t total = active * kMessages;
  std::chrono::nanoseconds elapsed{};
  for (int round = 0; round < kRounds; ++round) {
    // Prefill outside timing: isolate receive work from scheduling/publication.
    for (std::uint64_t seq = 0; seq < kMessages; ++seq) {
      Payload payload{seq};
      for (std::uint32_t p = 0; p < producers; ++p) {
        if (sparse && p + 1 != producers) {
          continue;
        }
        auto result = publishers[p].offer(std::as_bytes(std::span{&payload, 1}));
        if (!result || !*result) {
          std::cerr << "prefill failed" << std::endl;
          std::exit(2);
        }
      }
    }
    std::vector<std::uint64_t> expected(producers, 0);
    std::uint64_t global_sequence = static_cast<std::uint64_t>(round) * total;
    bool valid = true;
    const auto check = [&](const salias::Message& message) noexcept {
      std::uint64_t seq;
      std::memcpy(&seq, message.payload.data(), sizeof(seq));
      const auto p = message.producer_id;
      if (p >= producers || seq != expected[p]++) {
        valid = false;
      }
      if constexpr (M == salias::Mode::OrderedMpsc) {
        if (message.sequence != global_sequence++) {
          valid = false;
        }
      }
    };
    std::uint64_t received = 0;
    const auto start = Clock::now();
    while (received < total) {
      if constexpr (Batch) {
        const auto count = subscriber.poll(256, check);
        if (count == 0) {
          std::cerr << "batch receive stalled" << std::endl;
          std::exit(3);
        }
        received += count;
      } else {
        auto message = subscriber.try_recv();
        if (!message) {
          std::cerr << "receive stalled" << std::endl;
          std::exit(4);
        }
        check(*message);
        subscriber.release(*message);
        ++received;
      }
    }
    elapsed += std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start);
    if (!valid || received != total || subscriber.try_recv()) {
      std::cerr << "receive validation failed" << std::endl;
      std::exit(5);
    }
  }
  return static_cast<double>(elapsed.count()) / static_cast<double>(total * kRounds);
}

template <salias::Mode M, bool Batch>
void report(const char* mode, std::uint32_t producers, bool sparse, int reps) {
  (void)measure<M, Batch>(producers, sparse);
  std::vector<double> samples;
  for (int rep = 0; rep < reps; ++rep) {
    samples.push_back(measure<M, Batch>(producers, sparse));
  }
  std::sort(samples.begin(), samples.end());
  std::cout << "RESULT mode=" << mode << " producers=" << producers
            << " sparse=" << sparse << " batch=" << Batch
            << " median_ns=" << samples[samples.size() / 2]
            << " min_ns=" << samples.front() << " max_ns=" << samples.back() << std::endl;
}
}  // namespace

int main(int argc, char** argv) {
  const int reps = argc > 1 ? std::stoi(argv[1]) : 7;
  if (reps < 1 || reps > 100) {
    return 1;
  }
  for (const auto producers : {1u, 3u, 8u}) {
    for (const bool sparse : {false, true}) {
      if (sparse && producers == 1) {
        continue;
      }
      report<salias::Mode::FifoMpsc, false>("fifo", producers, sparse, reps);
      report<salias::Mode::OrderedMpsc, false>("ordered", producers, sparse, reps);
      report<salias::Mode::FifoMpsc, true>("fifo", producers, sparse, reps);
      report<salias::Mode::OrderedMpsc, true>("ordered", producers, sparse, reps);
    }
  }
}
