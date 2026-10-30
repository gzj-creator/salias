#include <benchmark/benchmark.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "salias/extras/latest_by_key_channel.hpp"

namespace {

using Clock = std::chrono::steady_clock;

struct Payload {
  std::uint64_t sequence = 0;
  std::uint64_t inverse = 0;
  std::array<std::byte, 48> padding{};
};

static_assert(sizeof(Payload) == 64, "the keyed benchmark payload must remain 64 bytes");

std::string unique_name() {
  static std::atomic<std::uint64_t> next{0};
  return "salias-keyed-bench-" + std::to_string(::getpid()) + "-" +
         std::to_string(next.fetch_add(1, std::memory_order_relaxed));
}

Payload make_payload(std::uint64_t writer, std::uint64_t sequence) noexcept {
  Payload payload{sequence, ~sequence, {}};
  // Keeping every byte deterministic catches torn or stale snapshots.  The first two fields are
  // intentionally redundant with the padding pattern so that a test cannot accidentally validate
  // only the sequence words.
  const auto seed = writer * 0x9e3779b97f4a7c15ULL ^ sequence;
  for (std::size_t index = 0; index < payload.padding.size(); ++index) {
    payload.padding[index] = static_cast<std::byte>(
        static_cast<unsigned char>((seed + index * 0x27U + (seed >> (index % 13))) & 0xffU));
  }
  return payload;
}

bool payload_matches(const Payload& payload, std::uint64_t writer,
                     std::uint64_t sequence) noexcept {
  const Payload expected = make_payload(writer, sequence);
  return payload.sequence == expected.sequence && payload.inverse == expected.inverse &&
         std::memcmp(payload.padding.data(), expected.padding.data(), expected.padding.size()) == 0;
}

struct WorkerStats {
  std::uint64_t published = 0;
  std::uint64_t delivered = 0;
  std::uint64_t attempts = 0;
  std::uint64_t backpressured = 0;
  std::uint64_t empty = 0;
  std::uint64_t retries = 0;
  std::uint64_t progress = 0;
  std::uint64_t invalid = 0;
  std::uint64_t cpu_ns = 0;
  std::uint64_t voluntary_context_switches = 0;
  std::uint64_t involuntary_context_switches = 0;
  std::uint64_t final_sequence = 0;
  int cpu_start = -1;
  int cpu_end = -1;
};

struct CpuUsage {
  std::uint64_t cpu_ns = 0;
  std::uint64_t voluntary_context_switches = 0;
  std::uint64_t involuntary_context_switches = 0;
};

CpuUsage read_cpu_usage() noexcept {
#if defined(__linux__)
  struct rusage usage {};
  if (::getrusage(RUSAGE_THREAD, &usage) != 0) {
    return {};
  }
  const auto to_ns = [](const timeval& value) noexcept {
    return static_cast<std::uint64_t>(value.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(value.tv_usec) * 1'000ULL;
  };
  return {.cpu_ns = to_ns(usage.ru_utime) + to_ns(usage.ru_stime),
          .voluntary_context_switches = static_cast<std::uint64_t>(usage.ru_nvcsw),
          .involuntary_context_switches = static_cast<std::uint64_t>(usage.ru_nivcsw)};
#else
  return {};
#endif
}

int current_cpu() noexcept {
#if defined(__linux__)
  return ::sched_getcpu();
#else
  return -1;
#endif
}

std::vector<int> configured_cpus() {
  const char* value = std::getenv("SALIAS_BENCH_CPUS");
  if (value == nullptr || *value == '\0') {
    return {};
  }
  std::vector<int> result;
  std::string_view text(value);
  while (!text.empty()) {
    while (!text.empty() && (text.front() == ',' || text.front() == ' ' || text.front() == '\t')) {
      text.remove_prefix(1);
    }
    if (text.empty()) {
      break;
    }
    int cpu = -1;
    const char* begin = text.data();
    const char* end = begin;
    while (end != text.data() + text.size() && *end >= '0' && *end <= '9') {
      ++end;
    }
    if (end == begin || std::from_chars(begin, end, cpu).ec != std::errc{} || cpu < 0) {
      return {};
    }
    result.push_back(cpu);
    text.remove_prefix(static_cast<std::size_t>(end - begin));
    while (!text.empty() && text.front() != ',') {
      if (text.front() != ' ' && text.front() != '\t') {
        return {};
      }
      text.remove_prefix(1);
    }
  }
  return result;
}

bool pin_current_thread(int cpu) noexcept {
#if defined(__linux__)
  if (cpu < 0 || cpu >= CPU_SETSIZE) {
    return false;
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  return ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set) == 0;
#else
  static_cast<void>(cpu);
  return true;
#endif
}

enum class RetryMode { LegacyRetry, AdaptiveRetry };

const char* retry_mode_name(RetryMode mode) noexcept {
  return mode == RetryMode::LegacyRetry ? "LegacyRetry" : "AdaptiveRetry";
}

template <RetryMode Mode>
class RetryBackoff {
 public:
  void progress() noexcept {
    relax_count_ = 0;
    delay_index_ = 0;
  }

  void wait(WorkerStats& stats) noexcept {
    ++stats.retries;
    if constexpr (Mode == RetryMode::LegacyRetry) {
      // This is the historical caller load and remains the primary keyed baseline.
      std::this_thread::yield();
    } else {
      // Keep the first sixteen retries identical to the old short-relax phase.  Once the
      // contention persists, use bounded 1/2/4/8/16/32/50 us sleeps and reset on progress.
      if (relax_count_ < 16) {
        ++relax_count_;
        std::this_thread::yield();
        return;
      }
      constexpr std::array<unsigned, 7> delays_us{1, 2, 4, 8, 16, 32, 50};
      const auto index = std::min(delay_index_, delays_us.size() - 1);
      ++delay_index_;
      std::this_thread::sleep_for(std::chrono::microseconds(delays_us[index]));
    }
  }

 private:
  unsigned relax_count_ = 0;
  std::size_t delay_index_ = 0;
};

void report_error(std::atomic<bool>& stop, std::atomic<int>& error,
                  salias::Error value) noexcept {
  int expected = 0;
  error.compare_exchange_strong(expected, static_cast<int>(value), std::memory_order_acq_rel,
                                std::memory_order_acquire);
  stop.store(true, std::memory_order_release);
}

template <RetryMode Mode, class Publisher>
bool publish_with_retry(Publisher& publisher, std::uint64_t key, const Payload& payload,
                        WorkerStats& stats, std::atomic<bool>& stop, std::atomic<int>& error,
                        const Clock::time_point deadline) noexcept {
  RetryBackoff<Mode> backoff;
  for (;;) {
    if (stop.load(std::memory_order_acquire) || Clock::now() >= deadline) {
      if (!stop.load(std::memory_order_acquire)) {
        report_error(stop, error, salias::Error::PlatformFail);
      }
      return false;
    }
    ++stats.attempts;
    auto offered = publisher.offer(key, payload);
    if (offered) {
      ++stats.published;
      backoff.progress();
      return true;
    }
    if (offered.error() != salias::Error::BackPressured) {
      ++stats.invalid;
      report_error(stop, error, offered.error());
      return false;
    }
    ++stats.backpressured;
    backoff.wait(stats);
  }
}

struct RunMetrics {
  bool valid = false;
  std::uint64_t elapsed_ns = 0;
  std::uint64_t publish_ns = 0;
  std::uint64_t drain_ns = 0;
  std::uint64_t published = 0;
  std::uint64_t delivered = 0;
  std::uint64_t attempts = 0;
  std::uint64_t backpressured = 0;
  std::uint64_t empty = 0;
  std::uint64_t retries = 0;
  std::uint64_t progress = 0;
  std::uint64_t invalid = 0;
  std::uint64_t cpu_ns = 0;
  std::uint64_t voluntary_context_switches = 0;
  std::uint64_t involuntary_context_switches = 0;
  std::uint64_t error = 0;
};

/**
 * A persistent four-writer keyed workload.
 *
 * Endpoint creation, thread creation, affinity setup, and readiness are deliberately outside the
 * Google Benchmark timer.  One generation processes four writers * 2000 updates; workers stay
 * alive between generations, so join, destruction, and unmapping are not charged to a message.
 */
template <std::size_t Readers, RetryMode Mode>
class ConcurrentKeyedRunner {
  static_assert(Readers > 0);
  using Channel = salias::extras::LatestByKeyChannel<std::uint64_t, Payload, 64>;
  using Publisher = typename Channel::Publisher;
  using Subscriber = typename Channel::Subscriber;

  static constexpr std::size_t kWriters = 4;
  static constexpr std::uint64_t kUpdates = 2'000;
  static constexpr auto kWatchdog = std::chrono::seconds(5);

 public:
  explicit ConcurrentKeyedRunner(Channel channel, std::vector<int> cpus)
      : channel_(std::move(channel)), cpus_(std::move(cpus)) {
    publishers_.reserve(kWriters);
    for (std::size_t index = 0; index < kWriters; ++index) {
      publishers_.push_back(channel_.publisher());
    }
    subscribers_.reserve(Readers);
    for (std::size_t index = 0; index < Readers; ++index) {
      subscribers_.push_back(channel_.subscriber());
    }
    stats_.resize(kWriters + Readers);
    threads_.reserve(kWriters + Readers);
    for (std::size_t index = 0; index < kWriters + Readers; ++index) {
      threads_.emplace_back([this, index] { worker(index); });
    }
    std::unique_lock lock(ready_mutex_);
    ready_cv_.wait_for(lock, kWatchdog, [this] {
      return ready_count_.load(std::memory_order_acquire) == stats_.size();
    });
    startup_ok_ = ready_count_.load(std::memory_order_acquire) == stats_.size() &&
                  !startup_failed_.load(std::memory_order_acquire);
  }

  ConcurrentKeyedRunner(const ConcurrentKeyedRunner&) = delete;
  ConcurrentKeyedRunner& operator=(const ConcurrentKeyedRunner&) = delete;

  ~ConcurrentKeyedRunner() noexcept {
    {
      std::lock_guard lock(control_mutex_);
      stopping_.store(true, std::memory_order_release);
      generation_.fetch_add(1, std::memory_order_release);
    }
    control_cv_.notify_all();
    for (auto& thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

  bool startup_ok() const noexcept { return startup_ok_; }
  bool affinity_requested() const noexcept { return !cpus_.empty(); }

  RunMetrics run(std::uint64_t run_index) noexcept {
    RunMetrics metrics{};
    if (!startup_ok_) {
      metrics.error = static_cast<std::uint64_t>(salias::Error::PlatformFail);
      return metrics;
    }
    for (auto& stats : stats_) {
      stats = WorkerStats{};
    }
    writer_done_.store(0, std::memory_order_relaxed);
    finished_.store(0, std::memory_order_relaxed);
    stop_.store(false, std::memory_order_release);
    error_.store(0, std::memory_order_release);
    const auto base = run_index * kUpdates;
    base_sequence_.store(base, std::memory_order_release);
    publish_end_ns_.store(0, std::memory_order_release);
    const auto started = Clock::now();
    const auto start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              started.time_since_epoch())
                              .count();
    run_start_ns_.store(static_cast<std::uint64_t>(start_ns), std::memory_order_release);
    {
      std::lock_guard lock(control_mutex_);
      generation_.fetch_add(1, std::memory_order_release);
    }
    control_cv_.notify_all();

    {
      std::unique_lock lock(control_mutex_);
      const bool finished = control_cv_.wait_for(lock, kWatchdog, [this] {
        return finished_.load(std::memory_order_acquire) == stats_.size();
      });
      if (!finished) {
        report_error(stop_, error_, salias::Error::PlatformFail);
      }
    }
    // A worker that noticed the stop flag may need one scheduler turn to leave its retry loop.
    if (finished_.load(std::memory_order_acquire) != stats_.size()) {
      control_cv_.notify_all();
      std::unique_lock lock(control_mutex_);
      control_cv_.wait_for(lock, std::chrono::seconds(1), [this] {
        return finished_.load(std::memory_order_acquire) == stats_.size();
      });
    }
    const auto ended = Clock::now();
    const auto end_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(ended.time_since_epoch())
                            .count();
    const auto publish_end = publish_end_ns_.load(std::memory_order_acquire);
    metrics.elapsed_ns = static_cast<std::uint64_t>(std::max<std::int64_t>(0, end_ns - start_ns));
    const auto publish_stop = publish_end == 0 ? static_cast<std::uint64_t>(end_ns) : publish_end;
    metrics.publish_ns = publish_stop > static_cast<std::uint64_t>(start_ns)
                             ? publish_stop - static_cast<std::uint64_t>(start_ns)
                             : 0;
    metrics.drain_ns = metrics.elapsed_ns > metrics.publish_ns ? metrics.elapsed_ns - metrics.publish_ns
                                                                : 0;
    for (const auto& stats : stats_) {
      metrics.published += stats.published;
      metrics.delivered += stats.delivered;
      metrics.attempts += stats.attempts;
      metrics.backpressured += stats.backpressured;
      metrics.empty += stats.empty;
      metrics.retries += stats.retries;
      metrics.progress += stats.progress;
      metrics.invalid += stats.invalid;
      metrics.cpu_ns += stats.cpu_ns;
      metrics.voluntary_context_switches += stats.voluntary_context_switches;
      metrics.involuntary_context_switches += stats.involuntary_context_switches;
    }
    metrics.error = static_cast<std::uint64_t>(error_.load(std::memory_order_acquire));
    metrics.valid = metrics.error == 0 && metrics.published == kWriters * kUpdates &&
                    finished_.load(std::memory_order_acquire) == stats_.size();
    return metrics;
  }

  static constexpr std::uint64_t updates_per_run() noexcept { return kUpdates; }
  static constexpr std::size_t writer_count() noexcept { return kWriters; }

 private:
  void mark_finished() noexcept {
    finished_.fetch_add(1, std::memory_order_release);
    control_cv_.notify_all();
  }

  void worker(std::size_t index) noexcept {
    bool affinity_ok = true;
    if (!cpus_.empty()) {
      affinity_ok = pin_current_thread(cpus_[index % cpus_.size()]);
    }
    if (!affinity_ok) {
      startup_failed_.store(true, std::memory_order_release);
    }
    ready_count_.fetch_add(1, std::memory_order_release);
    ready_cv_.notify_all();
    std::uint64_t local_generation = 0;
    for (;;) {
      std::unique_lock lock(control_mutex_);
      control_cv_.wait(lock, [this, local_generation] {
        return stopping_.load(std::memory_order_acquire) ||
               generation_.load(std::memory_order_acquire) != local_generation;
      });
      if (stopping_.load(std::memory_order_acquire)) {
        return;
      }
      local_generation = generation_.load(std::memory_order_acquire);
      lock.unlock();
      run_worker(index);
      mark_finished();
    }
  }

  void run_worker(std::size_t index) noexcept {
    const auto cpu_before = current_cpu();
    const auto usage_before = read_cpu_usage();
    WorkerStats& stats = stats_[index];
    if (index < kWriters) {
      auto publisher = publishers_[index];
      const auto base = base_sequence_.load(std::memory_order_acquire);
      const auto deadline = Clock::now() + kWatchdog;
      for (std::uint64_t sequence = 1; sequence <= kUpdates; ++sequence) {
        if (stop_.load(std::memory_order_acquire)) {
          break;
        }
        const auto absolute = base + sequence;
        const auto payload = make_payload(index, absolute);
        if (!publish_with_retry<Mode>(publisher, index, payload, stats, stop_, error_, deadline)) {
          break;
        }
        stats.progress = sequence;
        stats.final_sequence = absolute;
      }
      const auto published = stats.published;
      if (!stop_.load(std::memory_order_acquire) && published != kUpdates) {
        ++stats.invalid;
        report_error(stop_, error_, salias::Error::PlatformFail);
      }
      if (published == kUpdates) {
        const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             Clock::now().time_since_epoch())
                             .count();
        auto previous = publish_end_ns_.load(std::memory_order_relaxed);
        while (previous < static_cast<std::uint64_t>(now) &&
               !publish_end_ns_.compare_exchange_weak(previous, static_cast<std::uint64_t>(now),
                                                       std::memory_order_release,
                                                       std::memory_order_relaxed)) {
        }
      }
      writer_done_.fetch_add(1, std::memory_order_release);
    } else {
      auto subscriber = subscribers_[index - kWriters];
      std::array<std::uint64_t, kWriters> versions{};
      const auto base = base_sequence_.load(std::memory_order_acquire);
      versions.fill(base);
      const auto expected_final = base + kUpdates;
      RetryBackoff<Mode> backoff;
      const auto deadline = Clock::now() + kWatchdog;
      for (;;) {
        if (stop_.load(std::memory_order_acquire)) {
          break;
        }
        if (Clock::now() >= deadline) {
          ++stats.invalid;
          report_error(stop_, error_, salias::Error::PlatformFail);
          break;
        }
        const auto received = subscriber.try_recv_checked();
        if (!received) {
          if (received.error() != salias::Error::BackPressured) {
            ++stats.invalid;
            report_error(stop_, error_, received.error());
            break;
          }
          ++stats.backpressured;
          backoff.wait(stats);
          continue;
        }
        if (!*received) {
          ++stats.empty;
          if (writer_done_.load(std::memory_order_acquire) == kWriters) {
            bool complete = true;
            for (const auto version : versions) {
              complete = complete && version == expected_final;
            }
            if (complete) {
              break;
            }
          }
          backoff.wait(stats);
          continue;
        }
        const auto& message = **received;
        if (message.key >= kWriters || message.version != message.value.sequence ||
            message.version <= versions[message.key] || message.version > expected_final ||
            !payload_matches(message.value, message.key, message.value.sequence)) {
          ++stats.invalid;
          report_error(stop_, error_, salias::Error::PlatformFail);
          break;
        }
        versions[message.key] = message.version;
        ++stats.delivered;
        ++stats.progress;
        backoff.progress();
      }
      bool complete = true;
      for (const auto version : versions) {
        complete = complete && version == expected_final;
      }
      if (!complete && !stop_.load(std::memory_order_acquire)) {
        ++stats.invalid;
        report_error(stop_, error_, salias::Error::PlatformFail);
      }
      stats.final_sequence = complete ? expected_final : 0;
    }
    const auto usage_after = read_cpu_usage();
    stats.cpu_ns = usage_after.cpu_ns >= usage_before.cpu_ns
                       ? usage_after.cpu_ns - usage_before.cpu_ns
                       : 0;
    stats.voluntary_context_switches =
        usage_after.voluntary_context_switches >= usage_before.voluntary_context_switches
            ? usage_after.voluntary_context_switches - usage_before.voluntary_context_switches
            : 0;
    stats.involuntary_context_switches =
        usage_after.involuntary_context_switches >= usage_before.involuntary_context_switches
            ? usage_after.involuntary_context_switches - usage_before.involuntary_context_switches
            : 0;
    stats.cpu_start = cpu_before;
    stats.cpu_end = current_cpu();
  }

  Channel channel_;
  std::vector<int> cpus_;
  std::vector<Publisher> publishers_;
  std::vector<Subscriber> subscribers_;
  std::vector<WorkerStats> stats_;
  std::vector<std::thread> threads_;
  std::mutex control_mutex_;
  std::condition_variable control_cv_;
  std::mutex ready_mutex_;
  std::condition_variable ready_cv_;
  std::atomic<std::uint64_t> generation_{0};
  std::atomic<std::uint64_t> ready_count_{0};
  std::atomic<std::uint64_t> finished_{0};
  std::atomic<std::uint64_t> writer_done_{0};
  std::atomic<std::uint64_t> base_sequence_{0};
  std::atomic<std::uint64_t> run_start_ns_{0};
  std::atomic<std::uint64_t> publish_end_ns_{0};
  std::atomic<bool> stopping_{false};
  std::atomic<bool> stop_{false};
  std::atomic<bool> startup_failed_{false};
  std::atomic<int> error_{0};
  bool startup_ok_ = false;
};

template <std::size_t Readers, RetryMode Mode>
void BM_keyed_concurrent_64b(benchmark::State& state) {
  using Channel = salias::extras::LatestByKeyChannel<std::uint64_t, Payload, 64>;
  std::unique_ptr<ConcurrentKeyedRunner<Readers, Mode>> runner;
  std::uint64_t run_index = 0;
  std::uint64_t successful_runs = 0;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    // Google Benchmark does not start its timer until the range-for enters its first iteration.
    // Set up endpoints and workers there, while timing is paused, then keep those workers alive
    // for every subsequent generation.
    if (!runner) {
      state.PauseTiming();
      auto channel = Channel::create(unique_name());
      if (!channel) {
        state.ResumeTiming();
        state.SkipWithError("create failed");
        break;
      }
      runner = std::make_unique<ConcurrentKeyedRunner<Readers, Mode>>(
          std::move(*channel), configured_cpus());
      if (!runner->startup_ok()) {
        const bool requested = runner->affinity_requested();
        state.ResumeTiming();
        state.SkipWithError(requested ? "worker affinity setup failed" : "worker startup failed");
        break;
      }
      state.ResumeTiming();
    }
    const auto metrics = runner->run(run_index++);
    if (!metrics.valid) {
      state.SkipWithError("keyed worker validation/watchdog failure");
      break;
    }
    ++successful_runs;
    // The manual timer is the wall interval from the synchronized worker start until all readers
    // observed every writer's final version.  Worker join and endpoint destruction happen after
    // PauseTiming below and are never included.
    state.SetIterationTime(static_cast<double>(metrics.elapsed_ns) / 1e9);
    state.counters["publish_ns"] = static_cast<double>(metrics.publish_ns);
    state.counters["drain_ns"] = static_cast<double>(metrics.drain_ns);
    state.counters["publish_msg_per_sec"] =
        metrics.publish_ns == 0
            ? 0.0
            : static_cast<double>(ConcurrentKeyedRunner<Readers, Mode>::writer_count() *
                                  ConcurrentKeyedRunner<Readers, Mode>::updates_per_run()) /
                  (static_cast<double>(metrics.publish_ns) / 1e9);
    state.counters["drain_msg_per_sec"] =
        metrics.drain_ns == 0
            ? 0.0
            : static_cast<double>(metrics.delivered) / (static_cast<double>(metrics.drain_ns) / 1e9);
    state.counters["published"] = static_cast<double>(metrics.published);
    state.counters["delivered"] = static_cast<double>(metrics.delivered);
    state.counters["attempts"] = static_cast<double>(metrics.attempts);
    state.counters["backpressured"] = static_cast<double>(metrics.backpressured);
    state.counters["empty"] = static_cast<double>(metrics.empty);
    state.counters["progress"] = static_cast<double>(metrics.progress);
    state.counters["invalid"] = static_cast<double>(metrics.invalid);
    state.counters["worker_cpu_ms"] = static_cast<double>(metrics.cpu_ns) / 1e6;
    state.counters["context_switches"] = static_cast<double>(
        metrics.voluntary_context_switches + metrics.involuntary_context_switches);
  }
  // The range-for has stopped timing before this point.  Destroying the persistent workers now
  // keeps join, endpoint destruction and shared-memory unmapping outside the measured generations.
  runner.reset();
  state.SetLabel(std::string("retry=") + retry_mode_name(Mode) + ",readers=" +
                 std::to_string(Readers) + ",writers=4,updates=8000");
  state.SetItemsProcessed(static_cast<std::int64_t>(successful_runs) * 4 * 2'000);
  state.SetBytesProcessed(static_cast<std::int64_t>(successful_runs) * 4 * 2'000 * sizeof(Payload));
}

template <class Publisher>
bool publish_single(Publisher& publisher, std::uint64_t key, const Payload& payload) {
  const auto deadline = Clock::now() + std::chrono::seconds(5);
  for (;;) {
    const auto offered = publisher.offer(key, payload);
    if (offered) {
      return true;
    }
    if (offered.error() != salias::Error::BackPressured || Clock::now() >= deadline) {
      return false;
    }
    std::this_thread::yield();
  }
}

template <std::size_t Capacity, std::size_t Keys>
void BM_keyed_roundtrip_64b(benchmark::State& state) {
  using Channel = salias::extras::LatestByKeyChannel<std::uint64_t, Payload, Capacity>;
  auto channel = Channel::create(unique_name());
  if (!channel) {
    state.SkipWithError("create failed");
    return;
  }
  auto publisher = channel->publisher();
  auto subscriber = channel->subscriber();
  std::uint64_t sequence = 0;
  std::array<std::uint64_t, Keys> versions{};
  for (auto iteration : state) {
    static_cast<void>(iteration);
    ++sequence;
    const auto key = sequence % Keys;
    const auto payload = make_payload(key, sequence);
    const auto expected_version = ++versions[key];
    const auto offered = publisher.offer(key, payload);
    auto received = subscriber.try_recv_checked();
    if (!offered || !received || !*received || (**received).key != key ||
        (**received).value.sequence != sequence || (**received).version != expected_version ||
        !payload_matches((**received).value, key, sequence)) {
      state.SkipWithError("roundtrip validation failed");
      break;
    }
    benchmark::DoNotOptimize((**received).value);
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * sizeof(Payload));
}

template <std::size_t Capacity>
void BM_keyed_empty_poll(benchmark::State& state) {
  using Channel = salias::extras::LatestByKeyChannel<std::uint64_t, Payload, Capacity>;
  auto channel = Channel::create(unique_name());
  if (!channel) {
    state.SkipWithError("create failed");
    return;
  }
  auto subscriber = channel->subscriber();
  static_cast<void>(subscriber.try_recv_checked());
  for (auto iteration : state) {
    static_cast<void>(iteration);
    auto received = subscriber.try_recv_checked();
    if (!received || *received) {
      state.SkipWithError("empty poll validation failed");
      break;
    }
    benchmark::DoNotOptimize(received);
  }
  state.SetItemsProcessed(state.iterations());
}

void BM_keyed_ipc_roundtrip_64b(benchmark::State& state) {
  using Channel = salias::extras::LatestByKeyChannel<std::uint64_t, Payload, 16>;
  auto channel = Channel::create(unique_name());
  if (!channel) {
    state.SkipWithError("create failed");
    return;
  }
  auto publisher = channel->publisher();
  auto subscriber = channel->subscriber();
  const auto child = ::fork();
  if (child == 0) {
    auto deadline = Clock::now() + std::chrono::seconds(5);
    for (;;) {
      if (Clock::now() >= deadline) {
        ::_exit(4);
      }
      const auto received = subscriber.try_recv_checked();
      if (!received) {
        if (received.error() != salias::Error::BackPressured) {
          ::_exit(2);
        }
      } else if (*received && (**received).key == 0) {
        const auto sequence = (**received).value.sequence;
        if (sequence == std::numeric_limits<std::uint64_t>::max()) {
          ::_exit(0);
        }
        if (!payload_matches((**received).value, 0, sequence) ||
            !publish_single(publisher, 1, make_payload(1, sequence))) {
          ::_exit(3);
        }
        deadline = Clock::now() + std::chrono::seconds(5);
      }
    }
  }
  if (child < 0) {
    state.SkipWithError("fork failed");
    return;
  }
  std::uint64_t sequence = 0;
  bool valid = true;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    ++sequence;
    if (!publish_single(publisher, 0, make_payload(0, sequence))) {
      state.SkipWithError("ipc publish failed");
      valid = false;
      break;
    }
    const auto deadline = Clock::now() + std::chrono::seconds(5);
    for (;;) {
      const auto received = subscriber.try_recv_checked();
      if ((!received && received.error() != salias::Error::BackPressured) || Clock::now() >= deadline) {
        state.SkipWithError("ipc receive failed or timed out");
        valid = false;
        break;
      }
      if (received && *received && (**received).key == 1) {
        if ((**received).value.sequence != sequence || (**received).version != sequence ||
            !payload_matches((**received).value, 1, sequence)) {
          state.SkipWithError("ipc snapshot validation failed");
          valid = false;
        }
        break;
      }
    }
    if (!valid) {
      break;
    }
  }
  if (!valid || !publish_single(publisher, 0,
                                make_payload(0, std::numeric_limits<std::uint64_t>::max()))) {
    static_cast<void>(::kill(child, SIGKILL));
  }
  int status = 0;
  const auto deadline = Clock::now() + std::chrono::seconds(5);
  pid_t reaped = 0;
  for (;;) {
    reaped = ::waitpid(child, &status, WNOHANG);
    if (reaped == child || (reaped < 0 && errno != EINTR)) {
      break;
    }
    if (Clock::now() >= deadline) {
      static_cast<void>(::kill(child, SIGKILL));
      do {
        reaped = ::waitpid(child, &status, 0);
      } while (reaped < 0 && errno == EINTR);
      break;
    }
    std::this_thread::yield();
  }
  if (reaped != child || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    state.SkipWithError("ipc child failed");
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * 2 * sizeof(Payload));
}

BENCHMARK_TEMPLATE(BM_keyed_roundtrip_64b, 16, 1);
BENCHMARK_TEMPLATE(BM_keyed_roundtrip_64b, 4096, 1);
BENCHMARK_TEMPLATE(BM_keyed_roundtrip_64b, 4096, 64);
BENCHMARK_TEMPLATE(BM_keyed_empty_poll, 16);
BENCHMARK_TEMPLATE(BM_keyed_empty_poll, 4096);

// The fixed 4W/1R and 4W/4R workloads retain the original 8000-update load.  LegacyRetry is the
// acceptance baseline; AdaptiveRetry is a caller-side backoff comparison and must be reported
// separately, never presented as a storage-layer gain.
BENCHMARK_TEMPLATE(BM_keyed_concurrent_64b, 1, RetryMode::LegacyRetry)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_keyed_concurrent_64b, 1, RetryMode::AdaptiveRetry)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_keyed_concurrent_64b, 4, RetryMode::LegacyRetry)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_keyed_concurrent_64b, 4, RetryMode::AdaptiveRetry)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_keyed_ipc_roundtrip_64b)->UseRealTime();

}  // namespace

BENCHMARK_MAIN();
