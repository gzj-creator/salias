#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#ifdef __linux__
#include <sched.h>
#endif

namespace salias::final_bench {

inline constexpr std::size_t kPayloadSize = 64;

struct Options {
  std::string name;
  std::string mode = "fifo";
  std::filesystem::path coordination_dir;
  std::uint32_t index = 0;
  std::uint32_t producers = 2;
  std::uint32_t consumers = 1;
  std::uint64_t messages = 2'000'000;
  std::size_t capacity = 4u << 20;
  std::size_t batch_size = 1;
  std::uint32_t poll_limit = 64;
  int cpu = -1;
  bool create = false;
  bool aligned = false;
};

inline std::uint64_t monotonic_now_ns() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

inline std::uint64_t marker(std::uint32_t producer, std::uint64_t sequence) noexcept {
  return (static_cast<std::uint64_t>(producer) << 48) | (sequence & ((1ull << 48) - 1));
}

inline void write_payload(std::span<std::byte> payload, std::uint32_t producer,
                          std::uint64_t sequence) noexcept {
  const std::uint64_t value = marker(producer, sequence);
  std::memcpy(payload.data(), &value, sizeof(value));
  if (payload.size() > sizeof(value)) {
    std::memset(payload.data() + sizeof(value), 0, payload.size() - sizeof(value));
  }
}

inline std::uint64_t read_marker(std::span<const std::byte> payload) noexcept {
  std::uint64_t value = 0;
  if (payload.size() >= sizeof(value)) {
    std::memcpy(&value, payload.data(), sizeof(value));
  }
  return value;
}

inline void pin_cpu(int cpu) {
#ifdef __linux__
  if (cpu < 0) {
    return;
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  if (::sched_setaffinity(0, sizeof(set), &set) != 0) {
    throw std::runtime_error("sched_setaffinity failed");
  }
#else
  static_cast<void>(cpu);
#endif
}

inline void touch_file(const std::filesystem::path& path) {
  std::ofstream output(path);
  if (!output) {
    throw std::runtime_error("failed to create coordination file");
  }
}

inline void wait_for_file(const std::filesystem::path& path,
                          std::chrono::seconds timeout = std::chrono::seconds(60)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!std::filesystem::exists(path)) {
    if (std::chrono::steady_clock::now() >= deadline) {
      throw std::runtime_error("coordination timeout: " + path.string());
    }
    std::this_thread::yield();
  }
}

template <typename... Values>
void write_result(const std::filesystem::path& path, Values&&... values) {
  std::ofstream output(path);
  if (!output) {
    throw std::runtime_error("failed to write result file");
  }
  (output << ... << std::forward<Values>(values));
  output << '\n';
}

inline Options parse_options(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    auto value = [&](std::string_view name) -> std::string {
      if (++i >= argc) {
        throw std::runtime_error("missing value for " + std::string(name));
      }
      return argv[i];
    };
    if (arg == "--name") options.name = value(arg);
    else if (arg == "--mode") options.mode = value(arg);
    else if (arg == "--coord-dir") options.coordination_dir = value(arg);
    else if (arg == "--index") options.index = static_cast<std::uint32_t>(std::stoul(value(arg)));
    else if (arg == "--producers") options.producers = static_cast<std::uint32_t>(std::stoul(value(arg)));
    else if (arg == "--consumers") options.consumers = static_cast<std::uint32_t>(std::stoul(value(arg)));
    else if (arg == "--messages") options.messages = std::stoull(value(arg));
    else if (arg == "--capacity") options.capacity = std::stoull(value(arg));
    else if (arg == "--batch-size") options.batch_size = std::stoull(value(arg));
    else if (arg == "--poll-limit") options.poll_limit = static_cast<std::uint32_t>(std::stoul(value(arg)));
    else if (arg == "--cpu") options.cpu = std::stoi(value(arg));
    else if (arg == "--create") options.create = true;
    else if (arg == "--aligned") options.aligned = true;
    else throw std::runtime_error("unknown argument: " + std::string(arg));
  }
  if (options.name.empty() || options.coordination_dir.empty() || options.producers == 0 ||
      options.consumers == 0 || options.messages == 0 || options.capacity == 0 ||
      options.batch_size == 0 || options.poll_limit == 0) {
    throw std::runtime_error("invalid benchmark options");
  }
  return options;
}

}  // namespace salias::final_bench
