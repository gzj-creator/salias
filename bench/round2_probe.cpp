#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <sched.h>
#endif

#include "salias/salias.hpp"

namespace {

using Clock = std::chrono::steady_clock;

pid_t wait_child(pid_t child, int& status) {
  pid_t result = -1;
  do {
    result = ::waitpid(child, &status, 0);
  } while (result < 0 && errno == EINTR);
  return result;
}

std::uint64_t now_ns() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch())
          .count());
}

double thread_cpu() {
  timespec value{};
  if (::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value) != 0) {
    std::abort();
  }
  return static_cast<double>(value.tv_sec) + static_cast<double>(value.tv_nsec) / 1e9;
}

salias::Config config_for(std::string suffix, std::uint32_t writers = 1,
                          std::uint32_t readers = 1) {
  salias::Config config;
  config.name = "salias-round2-probe-" + std::to_string(::getpid()) + "-" + suffix;
  config.capacity = 4096;
  config.num_producers = writers;
  config.num_consumers = readers;
  return config;
}

void fill_payload(std::span<std::byte> payload, std::uint64_t sequence, std::uint32_t writer) {
  std::memcpy(payload.data(), &sequence, sizeof(sequence));
  for (std::size_t index = sizeof(sequence); index < payload.size(); ++index) {
    payload[index] = static_cast<std::byte>((sequence + writer * 17 + index) & 255);
  }
}

bool valid_payload(std::span<const std::byte> payload, std::uint64_t sequence,
                   std::uint32_t writer) {
  std::uint64_t actual = 0;
  if (payload.size() < sizeof(actual)) {
    return false;
  }
  std::memcpy(&actual, payload.data(), sizeof(actual));
  if (actual != sequence) {
    return false;
  }
  for (std::size_t index = sizeof(sequence); index < payload.size(); ++index) {
    if (payload[index] != static_cast<std::byte>((sequence + writer * 17 + index) & 255)) {
      return false;
    }
  }
  return true;
}

int reliable(double seconds, std::uint32_t writers, std::uint32_t readers, std::uint64_t window,
             std::size_t payload_size, std::uint32_t batch_size) {
  auto config = config_for("reliable", writers, readers);
  config.publication_window_messages = window;
  auto channel = salias::LatestMpmcChannel<4096, 50>::create(config);
  if (!channel) {
    return 2;
  }
  std::vector<salias::LatestMpmcPublisher> publishers;
  std::vector<salias::LatestMpmcSubscriber> subscribers;
  for (std::uint32_t index = 0; index < writers; ++index) {
    publishers.push_back(channel->publisher());
  }
  for (std::uint32_t index = 0; index < readers; ++index) {
    auto subscriber = channel->try_subscriber(salias::SubscriptionPolicy::Reliable);
    if (!subscriber) {
      return 3;
    }
    subscribers.push_back(std::move(*subscriber));
  }
  std::atomic<bool> started{false};
  std::atomic<bool> stop{false};
  std::atomic<bool> failed{false};
  std::atomic<std::uint32_t> finished_writers{0};
  std::vector<std::uint64_t> published(writers, 0);
  std::vector<std::uint64_t> received(readers, 0);
  std::vector<std::thread> threads;
  for (std::uint32_t index = 0; index < readers; ++index) {
    threads.emplace_back([&, index] {
      auto subscriber = std::move(subscribers[index]);
      std::vector<std::uint64_t> next(writers, 0);
      while (!started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      while (!failed.load(std::memory_order_relaxed)) {
        const bool all_writers_finished =
            finished_writers.load(std::memory_order_acquire) == writers;
        auto message = subscriber.try_recv_checked();
        if (!message) {
          if (message.error() != salias::Error::BackPressured) {
            failed.store(true, std::memory_order_relaxed);
          }
          continue;
        }
        if (!*message) {
          if (all_writers_finished) {
            if (next != published) {
              failed.store(true, std::memory_order_relaxed);
            }
            break;
          }
          std::this_thread::yield();
          continue;
        }
        const auto& item = **message;
        const auto writer = item.producer_id;
        if (writer >= writers || item.payload.size() != payload_size ||
            item.sequence != next[writer] || !valid_payload(item.payload, next[writer], writer)) {
          failed.store(true, std::memory_order_relaxed);
        } else {
          ++next[writer];
          ++received[index];
        }
        subscriber.release(item);
      }
    });
  }
  for (std::uint32_t index = 0; index < writers; ++index) {
    threads.emplace_back([&, index] {
      auto publisher = std::move(publishers[index]);
      std::vector<std::vector<std::byte>> payloads(batch_size,
                                                   std::vector<std::byte>(payload_size));
      std::vector<std::span<const std::byte>> views(batch_size);
      std::uint64_t sequence = 0;
      while (!started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      while (!stop.load(std::memory_order_relaxed) && !failed.load(std::memory_order_relaxed)) {
        for (std::uint32_t batch = 0; batch < batch_size; ++batch) {
          fill_payload(payloads[batch], sequence + batch, index);
          views[batch] = payloads[batch];
        }
        auto offered = publisher.offer_batch(views);
        if (offered) {
          sequence += *offered;
        } else if (offered.error() != salias::Error::BackPressured) {
          failed.store(true, std::memory_order_relaxed);
        } else {
          std::this_thread::yield();
        }
      }
      published[index] = sequence;
      finished_writers.fetch_add(1, std::memory_order_acq_rel);
    });
  }
  const auto begin = Clock::now();
  started.store(true, std::memory_order_release);
  std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
  stop.store(true, std::memory_order_relaxed);
  for (auto& thread : threads) {
    thread.join();
  }
  std::uint64_t total = 0;
  for (auto count : published) {
    total += count;
  }
  std::uint64_t deliveries = 0;
  for (auto count : received) {
    deliveries += count;
    if (count != total) {
      failed.store(true, std::memory_order_relaxed);
    }
  }
  const auto wall = std::chrono::duration<double>(Clock::now() - begin).count();
  std::printf(
      "{\"mode\":\"reliable\",\"seconds\":%.6f,\"writers\":%u,\"readers\":%u,"
      "\"window\":%llu,\"bytes\":%zu,\"batch\":%u,\"published\":%llu,"
      "\"deliveries\":%llu,\"valid\":%s}\n",
      wall, writers, readers, static_cast<unsigned long long>(window), payload_size, batch_size,
      static_cast<unsigned long long>(total), static_cast<unsigned long long>(deliveries),
      failed.load() || total == 0 ? "false" : "true");
  return failed.load() || total == 0 ? 4 : 0;
}

struct SharedProbe {
  std::atomic<bool> ready{false};
  std::atomic<bool> failed{false};
  std::atomic<std::uint64_t> acknowledged{0};
};

bool pin_thread(std::string_view topology, bool writer) {
#if defined(__linux__)
  if (topology != "same" && topology != "cores") {
    return true;
  }
  cpu_set_t allowed;
  CPU_ZERO(&allowed);
  if (::sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
    return false;
  }
  std::array<int, 2> chosen{-1, -1};
  std::size_t count = 0;
  for (int cpu = 0; cpu < CPU_SETSIZE && count < chosen.size(); ++cpu) {
    if (CPU_ISSET(cpu, &allowed)) {
      chosen[count++] = cpu;
    }
  }
  if (count == 0 || (topology == "cores" && count < 2)) {
    return false;
  }
  cpu_set_t requested;
  CPU_ZERO(&requested);
  CPU_SET(chosen[writer && topology == "cores" ? 1 : 0], &requested);
  return ::sched_setaffinity(0, sizeof(requested), &requested) == 0;
#else
  static_cast<void>(writer);
  return topology != "same" && topology != "cores";
#endif
}

template <std::size_t... Indices>
int priority(double seconds, int policy, std::uint64_t interval_microseconds,
             std::size_t payload_size, std::string_view topology, std::index_sequence<Indices...>) {
  constexpr std::size_t streams = sizeof...(Indices);
  using Reader =
      salias::PrioritySubscriber<(static_cast<void>(Indices), salias::Mode::FifoMpsc)...>;
  using Publisher =
      salias::PriorityPublisher<(static_cast<void>(Indices), salias::Mode::FifoMpsc)...>;
  std::vector<salias::FifoMpscChannel> channels;
  for (std::size_t index = 0; index < streams; ++index) {
    auto channel = salias::FifoMpscChannel::create(config_for("priority-" + std::to_string(index)));
    if (!channel) {
      return 2;
    }
    channels.push_back(std::move(*channel));
  }
  Reader reader(channels[Indices].subscriber()...);
  Publisher publisher(channels[Indices].publisher()...);
  void* memory = ::mmap(nullptr, sizeof(SharedProbe), PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (memory == MAP_FAILED) {
    return 3;
  }
  auto* shared = std::construct_at(static_cast<SharedProbe*>(memory));
  const auto writer = [&] {
    if (!pin_thread(topology, true)) {
      shared->failed.store(true, std::memory_order_release);
      return;
    }
    while (!shared->ready.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    const auto deadline = Clock::now() + std::chrono::duration<double>(seconds);
    std::vector<std::byte> payload(payload_size);
    std::uint64_t sequence = 0;
    for (;;) {
      while (shared->acknowledged.load(std::memory_order_acquire) != sequence) {
        if (shared->failed.load(std::memory_order_relaxed)) {
          return;
        }
        std::this_thread::yield();
      }
      const bool final = Clock::now() >= deadline;
      if (!final && interval_microseconds != 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(interval_microseconds));
      }
      fill_payload(payload, sequence, 0);
      const auto timestamp = final ? std::uint64_t{0} : now_ns();
      std::memcpy(payload.data(), &timestamp, sizeof(timestamp));
      auto offered = publisher.offer(sequence % streams, payload);
      if (!offered || !*offered) {
        shared->failed.store(true, std::memory_order_release);
        return;
      }
      if (final) {
        return;
      }
      ++sequence;
    }
  };
  pid_t child = -1;
  std::thread worker;
  if (topology == "process") {
    child = ::fork();
    if (child == 0) {
      ::alarm(static_cast<unsigned>(seconds + 15));
      writer();
      ::_exit(shared->failed.load() ? 4 : 0);
    }
    if (child < 0) {
      return 4;
    }
  } else {
    worker = std::thread(writer);
  }
  const bool pinned = pin_thread(topology, false);
  if (!pinned) {
    shared->failed.store(true, std::memory_order_release);
  }
  std::vector<std::uint64_t> latency;
  std::array<std::uint64_t, streams> per_stream{};
  std::uint64_t sequence = 0;
  const auto begin = Clock::now();
  const auto cpu_begin = thread_cpu();
  shared->ready.store(true, std::memory_order_release);
  while (!shared->failed.load(std::memory_order_relaxed)) {
#ifdef SALIAS_ROUND2_BASELINE
    auto message = reader.recv();
    static_cast<void>(policy);
#else
    auto message = reader.recv(static_cast<salias::PriorityWaitPolicy>(policy));
#endif
    const auto received_at = now_ns();
    if (!message || message->message.payload.size() != payload_size) {
      shared->failed.store(true, std::memory_order_release);
      break;
    }
    std::uint64_t timestamp = 0;
    std::memcpy(&timestamp, message->message.payload.data(), sizeof(timestamp));
    if (message->priority != sequence % streams ||
        message->message.sequence != per_stream[message->priority]++) {
      shared->failed.store(true, std::memory_order_release);
    }
    for (std::size_t index = sizeof(timestamp); index < payload_size; ++index) {
      if (message->message.payload[index] != static_cast<std::byte>((sequence + index) & 255)) {
        shared->failed.store(true, std::memory_order_release);
      }
    }
    if (!reader.release(*message)) {
      shared->failed.store(true, std::memory_order_release);
    }
    if (timestamp == 0) {
      break;
    }
    if (timestamp > received_at) {
      shared->failed.store(true, std::memory_order_release);
      break;
    }
    latency.push_back(received_at - timestamp);
    ++sequence;
    shared->acknowledged.store(sequence, std::memory_order_release);
  }
  const auto cpu = thread_cpu() - cpu_begin;
  const auto wall = std::chrono::duration<double>(Clock::now() - begin).count();
  if (worker.joinable()) {
    worker.join();
  }
  if (child > 0) {
    int status = 0;
    if (wait_child(child, status) != child || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      shared->failed.store(true, std::memory_order_release);
    }
  }
  std::sort(latency.begin(), latency.end());
  const auto percentile = [&](double fraction) {
    return latency.empty() ? std::uint64_t{0}
                           : latency[static_cast<std::size_t>((latency.size() - 1) * fraction)];
  };
  const bool valid = !shared->failed.load() && sequence > 0;
  std::printf(
      "{\"mode\":\"priority\",\"policy\":%d,\"topology\":\"%s\",\"streams\":%zu,"
      "\"bytes\":%zu,\"interval_us\":%llu,\"seconds\":%.6f,\"cpu_seconds\":%.6f,"
      "\"cpu_percent\":%.3f,\"delivered\":%llu,\"p50_ns\":%llu,\"p99_ns\":%llu,"
      "\"p999_ns\":%llu,\"valid\":%s,\"baseline_yield\":%s}\n",
      policy, std::string(topology).c_str(), streams, payload_size,
      static_cast<unsigned long long>(interval_microseconds), wall, cpu, cpu / wall * 100,
      static_cast<unsigned long long>(sequence), static_cast<unsigned long long>(percentile(0.5)),
      static_cast<unsigned long long>(percentile(0.99)),
      static_cast<unsigned long long>(percentile(0.999)), valid ? "true" : "false",
#ifdef SALIAS_ROUND2_BASELINE
      "true"
#else
      "false"
#endif
  );
  std::destroy_at(shared);
  static_cast<void>(::munmap(memory, sizeof(SharedProbe)));
  return valid ? 0 : 5;
}

int priority_saturated(double seconds, int policy) {
  auto high = salias::FifoMpscChannel::create(config_for("saturated-high"));
  auto low = salias::FifoMpscChannel::create(config_for("saturated-low"));
  if (!high || !low) {
    return 2;
  }
  salias::PrioritySubscriber<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc> reader(
      high->subscriber(), low->subscriber());
  salias::PriorityPublisher<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc> publisher(
      high->publisher(), low->publisher());
  std::atomic<bool> ready{false};
  std::atomic<bool> failed{false};
  std::array<std::uint64_t, 2> published{};
  std::array<std::uint64_t, 2> received{};
  constexpr auto end_marker = std::numeric_limits<std::uint64_t>::max();
  std::thread writer([&] {
    while (!ready.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    const auto deadline = Clock::now() + std::chrono::duration<double>(seconds);
    std::array<std::byte, 64> payload{};
    const auto offer = [&](std::size_t priority, std::uint64_t sequence) {
      fill_payload(payload, sequence, static_cast<std::uint32_t>(priority));
      while (!failed.load(std::memory_order_relaxed)) {
        auto result = publisher.offer(priority, payload);
        if (result && *result) {
          return true;
        }
        if (!result && result.error() != salias::Error::BackPressured) {
          failed.store(true, std::memory_order_relaxed);
          return false;
        }
        std::this_thread::yield();
      }
      return false;
    };
    std::size_t priority = 0;
    while (Clock::now() < deadline && !failed.load(std::memory_order_relaxed)) {
      if (!offer(priority, published[priority])) {
        return;
      }
      ++published[priority];
      priority = (priority + 1) % published.size();
    }
    for (std::size_t index = 0; index < published.size(); ++index) {
      if (!offer(index, end_marker)) {
        return;
      }
    }
  });
  std::array<bool, 2> finished{};
  std::uint32_t finished_streams = 0;
  const auto begin = Clock::now();
  const auto cpu_begin = thread_cpu();
  ready.store(true, std::memory_order_release);
  while (finished_streams != 2 && !failed.load(std::memory_order_relaxed)) {
#ifdef SALIAS_ROUND2_BASELINE
    auto message = reader.recv();
    static_cast<void>(policy);
#else
    auto message = reader.recv(static_cast<salias::PriorityWaitPolicy>(policy));
#endif
    if (!message || message->priority >= received.size() || message->message.payload.size() != 64) {
      failed.store(true, std::memory_order_relaxed);
      break;
    }
    const auto priority = message->priority;
    std::uint64_t sequence = 0;
    std::memcpy(&sequence, message->message.payload.data(), sizeof(sequence));
    if (finished[priority] || message->message.sequence != received[priority] ||
        !valid_payload(message->message.payload, sequence, priority)) {
      failed.store(true, std::memory_order_relaxed);
    } else if (sequence == end_marker) {
      finished[priority] = true;
      ++finished_streams;
    } else if (sequence != received[priority]) {
      failed.store(true, std::memory_order_relaxed);
    } else {
      ++received[priority];
    }
    if (!reader.release(*message)) {
      failed.store(true, std::memory_order_relaxed);
    }
  }
  const auto cpu = thread_cpu() - cpu_begin;
  const auto wall = std::chrono::duration<double>(Clock::now() - begin).count();
  writer.join();
  const auto total = received[0] + received[1];
  const bool valid = !failed.load() && received == published && total != 0;
  std::printf(
      "{\"mode\":\"priority-saturated\",\"policy\":%d,\"seconds\":%.6f,"
      "\"cpu_seconds\":%.6f,\"delivered\":%llu,\"messages_per_second\":%.3f,"
      "\"valid\":%s}\n",
      policy, wall, cpu, static_cast<unsigned long long>(total), total / wall,
      valid ? "true" : "false");
  return valid ? 0 : 5;
}

int trace(std::uint64_t count, std::uint64_t window) {
  auto config = config_for("trace");
  config.publication_window_messages = window;
  auto channel = salias::LatestMpmcChannel<4096, 50>::create(config);
  if (!channel) {
    return 2;
  }
  auto publisher = channel->publisher();
  auto subscriber = channel->subscriber(salias::SubscriptionPolicy::Reliable);
  std::array<std::byte, 64> payload{};
  for (std::uint64_t sequence = 0; sequence < count; ++sequence) {
    fill_payload(payload, sequence, 0);
    auto offered = publisher.offer(payload);
    auto message = subscriber.try_recv_checked();
    if (!offered || !*offered || !message || !*message ||
        !valid_payload((*message)->payload, sequence, 0)) {
      return 3;
    }
    subscriber.release(**message);
  }
  std::printf("{\"mode\":\"trace\",\"count\":%llu,\"window\":%llu,\"valid\":true}\n",
              static_cast<unsigned long long>(count), static_cast<unsigned long long>(window));
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc == 4 && std::string_view(argv[1]) == "priority-saturated") {
    const double seconds = std::strtod(argv[2], nullptr);
    const int policy = static_cast<int>(std::strtol(argv[3], nullptr, 10));
    if (!(seconds > 0 && seconds <= 3600) || policy < 0 || policy > 2) {
      return 1;
    }
    ::alarm(static_cast<unsigned>(seconds + 20));
    return priority_saturated(seconds, policy);
  }
  if (argc == 4 && std::string_view(argv[1]) == "trace") {
    ::alarm(60);
    return trace(std::strtoull(argv[2], nullptr, 10), std::strtoull(argv[3], nullptr, 10));
  }
  if (argc != 8) {
    std::fprintf(stderr,
                 "usage: %s reliable seconds writers readers window bytes batch\n"
                 "       %s priority seconds policy interval_us bytes streams topology\n"
                 "       %s trace iterations window\n"
                 "       %s priority-saturated seconds policy\n"
                 "policy: 0=Yield, 1=Spin, 2=Adaptive; topology: thread/process/same/cores\n",
                 argv[0], argv[0], argv[0], argv[0]);
    return 1;
  }
  const double seconds = std::strtod(argv[2], nullptr);
  if (!(seconds > 0 && seconds <= 3600)) {
    return 1;
  }
  ::alarm(static_cast<unsigned>(seconds + 20));
  if (std::string_view(argv[1]) == "reliable") {
    const auto writers = static_cast<std::uint32_t>(std::strtoul(argv[3], nullptr, 10));
    const auto readers = static_cast<std::uint32_t>(std::strtoul(argv[4], nullptr, 10));
    const auto window = std::strtoull(argv[5], nullptr, 10);
    const auto bytes = std::strtoull(argv[6], nullptr, 10);
    const auto batch = static_cast<std::uint32_t>(std::strtoul(argv[7], nullptr, 10));
    if (writers == 0 || writers > 4 || readers == 0 || readers > 8 || bytes < 8 || bytes > 1024 ||
        batch == 0 || batch > 16 || (((bytes + 15) / 8) * 8) * batch > 2048 ||
        (window != 0 && batch > window)) {
      return 1;
    }
    return reliable(seconds, writers, readers, window, static_cast<std::size_t>(bytes), batch);
  }
  if (std::string_view(argv[1]) == "priority") {
    const int policy = static_cast<int>(std::strtol(argv[3], nullptr, 10));
    const auto interval = std::strtoull(argv[4], nullptr, 10);
    const auto bytes = std::strtoull(argv[5], nullptr, 10);
    const int streams = static_cast<int>(std::strtol(argv[6], nullptr, 10));
    const std::string_view topology(argv[7]);
    if (policy < 0 || policy > 2 || interval > 10000000 || bytes < 8 || bytes > 1024 ||
        (topology != "thread" && topology != "process" && topology != "same" &&
         topology != "cores")) {
      return 1;
    }
    if (streams == 2) {
      return priority(seconds, policy, interval, bytes, topology, std::make_index_sequence<2>{});
    }
    if (streams == 3) {
      return priority(seconds, policy, interval, bytes, topology, std::make_index_sequence<3>{});
    }
    if (streams == 8) {
      return priority(seconds, policy, interval, bytes, topology, std::make_index_sequence<8>{});
    }
  }
  return 1;
}
