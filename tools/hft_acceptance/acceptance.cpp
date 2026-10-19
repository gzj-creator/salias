// Synthetic, open-loop, cross-process FIFO acceptance. No product changes.
#include <sched.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <new>
#include <numeric>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "salias/salias.hpp"
#include "../aeron_compare/latency_histogram.hpp"

namespace {
namespace lat = salias::tools::latency;
constexpr std::uint64_t second = 1'000'000'000;
constexpr std::uint64_t budget_ns = 50'000;
constexpr std::uint64_t magic = 0x53414c4941534846ULL;
volatile sig_atomic_t interrupted = 0;
void on_signal(int) { interrupted = 1; }
std::uint64_t now_ns() { return lat::monotonic_now_ns(); }
void pause_cpu() { __builtin_ia32_pause(); }
void pin(int cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  if (sched_setaffinity(0, sizeof(set), &set)) throw std::runtime_error("CPU pin failed");
}
std::uint64_t hash(std::uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}
bool sampled(std::uint64_t seq, unsigned publisher) {
  return (hash(seq ^ (std::uint64_t{publisher} << 61)) & 63) == 0;
}

struct Histogram {
  std::array<std::uint64_t, lat::kBucketCount> buckets{};
  std::uint64_t samples = 0, max = 0, above_budget = 0;
  void record(std::uint64_t ns) {
    lat::record(buckets.data(), ns);
    ++samples;
    max = std::max(max, ns);
    above_budget += ns > budget_ns;
  }
  void merge(const Histogram& rhs) {
    lat::merge(buckets.data(), rhs.buckets.data());
    samples += rhs.samples;
    max = std::max(max, rhs.max);
    above_budget += rhs.above_budget;
  }
  bool passes(unsigned n, unsigned d) const {
    const auto rank = samples / d * n + ((samples % d) * n + d - 1) / d;
    return samples && samples - above_budget >= rank;
  }
  void print() const {
    auto p = [&](unsigned n, unsigned d) {
      return lat::percentile(buckets.data(), samples, n, d);
    };
    std::cout << "{\"samples\":" << samples << ",\"p50_ns\":" << p(50, 100)
              << ",\"p99_ns\":" << p(99, 100) << ",\"p999_ns\":" << p(999, 1000)
              << ",\"max_sampled_ns\":" << max << ",\"above_50us\":" << above_budget
              << ",\"p99_pass_50us\":" << (passes(99, 100) ? "true" : "false")
              << ",\"p999_pass_50us\":" << (passes(999, 1000) ? "true" : "false")
              << ",\"buckets\":[";
    bool comma = false;
    for (std::size_t i = 0; i < buckets.size(); ++i) {
      if (!buckets[i]) continue;
      if (comma) std::cout << ',';
      std::cout << '[' << i << ',' << buckets[i] << ']';
      comma = true;
    }
    std::cout << "]}";
  }
};

struct Options {
  unsigned subscribers = 1, payload = 64, seconds = 225, cpu_base = 0;
  std::uint64_t rate = 1'000'000, peak_rate = 4'000'000;
  unsigned burst_ms = 100;
  bool burst = false;
  bool diagnostics = false;
  unsigned stall_subscriber_ms = 0;
};

// Worker-local snapshots: no monitoring reads or extra clocks on the formal hot path.
struct Resources {
  rusage start{}, finish{};
  void begin() { getrusage(RUSAGE_SELF, &start); }
  void end() { getrusage(RUSAGE_SELF, &finish); }
  void print() const {
    const auto micros = [](timeval t) { return std::int64_t{t.tv_sec} * 1'000'000 + t.tv_usec; };
    std::cout << "{\"user_us\":" << micros(finish.ru_utime) - micros(start.ru_utime)
              << ",\"system_us\":" << micros(finish.ru_stime) - micros(start.ru_stime)
              << ",\"voluntary_context_switches\":" << finish.ru_nvcsw - start.ru_nvcsw
              << ",\"involuntary_context_switches\":" << finish.ru_nivcsw - start.ru_nivcsw
              << ",\"minor_faults\":" << finish.ru_minflt - start.ru_minflt
              << ",\"major_faults\":" << finish.ru_majflt - start.ru_majflt << '}';
  }
};
// The schedule never shifts when the writer falls behind. Unsent arrivals remain
// explicit at cutoff; only already-published frames are drained afterwards.
struct Schedule {
  std::uint64_t low, high, burst_ns, cycle_messages, duration_ns;
  explicit Schedule(const Options& o)
      : low(o.rate / 2), high(o.burst ? o.peak_rate / 2 : o.rate / 2),
        burst_ns(o.burst ? std::uint64_t{o.burst_ms} * 1'000'000 : 0),
        cycle_messages(high * burst_ns / second + low * (second - burst_ns) / second),
        duration_ns(std::uint64_t{o.seconds} * second) {}
  std::uint64_t total() const { return cycle_messages * (duration_ns / second); }
  std::uint64_t due(std::uint64_t seq) const {
    const auto cycle = seq / cycle_messages;
    const auto offset = seq % cycle_messages;
    const auto peak_count = high * burst_ns / second;
    return cycle * second + (offset < peak_count ? offset * second / high
        : burst_ns + (offset - peak_count) * second / low);
  }
  std::uint64_t arrivals(std::uint64_t elapsed) const {
    if (elapsed >= duration_ns) return total();
    const auto cycle = elapsed / second, part = elapsed % second;
    const auto count = cycle * cycle_messages + (part < burst_ns
        ? part * high / second : high * burst_ns / second + (part - burst_ns) * low / second);
    return std::min(total(), count + 1);
  }
};

struct Header {
  std::uint64_t marker, sequence, planned_ns, attempt_ns;
  std::uint32_t publisher, bytes;
  std::uint64_t check;
};
static_assert(sizeof(Header) == 48);
std::uint64_t check_header(const Header& h) {
  return h.marker ^ h.sequence ^ h.planned_ns ^ h.attempt_ns ^
      (std::uint64_t{h.publisher} << 32) ^ h.bytes ^ 0xd5b4a93271c60e8fULL;
}
struct alignas(64) PublisherStats {
  std::atomic<std::uint64_t> sent{0};
  std::atomic<bool> done{false};
  std::uint64_t retries = 0, bp_messages = 0, bp_ns = 0, bp_max_ns = 0;
  std::uint64_t backlog_highwater_observed = 0, ended_ns = 0;
  Histogram claim_commit, schedule_lag;
  Histogram successful_claim, failed_claim, commit_call;
  Resources resources;
};
struct SequenceStats {
  std::uint64_t received = 0, unique = 0, duplicate = 0, out_of_order = 0;
  std::uint64_t gap_events = 0, gap_positions_observed = 0, high_sequence_next = 0;
  std::uint64_t corrupt = 0, invalid = 0;
  int channel_producer_id = -1;
};
void observe_sequence(SequenceStats& seq, std::vector<std::uint64_t>& seen, std::uint64_t value) {
  auto& bits = seen[value / 64];
  const auto mask = std::uint64_t{1} << (value % 64);
  const bool duplicate = (bits & mask) != 0;
  if (duplicate) ++seq.duplicate;
  else { bits |= mask; ++seq.unique; }
  if (value > seq.high_sequence_next) {
    ++seq.gap_events;
    seq.gap_positions_observed += value - seq.high_sequence_next;
  }
  if (value < seq.high_sequence_next && !duplicate) ++seq.out_of_order;
  seq.high_sequence_next = std::max(seq.high_sequence_next, value + 1);
}
struct alignas(64) SubscriberStats {
  std::atomic<std::uint64_t> received{0};
  std::array<SequenceStats, 2> sequence{};
  std::uint64_t invalid_publisher = 0, clock_reversals = 0, ended_ns = 0;
  std::uint64_t unreceived_at_measurement_cutoff = 0;
  Histogram from_attempt, from_schedule;
  Resources resources;
};
struct Shared {
  std::atomic<unsigned> ready{0};
  std::atomic<bool> start{false}, failed{false}, stop{false};
  std::uint64_t start_ns = 0;
  std::array<PublisherStats, 2> publishers{};
  std::array<SubscriberStats, 2> subscribers{};
};
static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
static_assert(std::atomic<unsigned>::is_always_lock_free);
static_assert(std::atomic<bool>::is_always_lock_free);

void ready(Shared& s) {
  s.ready.fetch_add(1, std::memory_order_release);
  while (!s.start.load(std::memory_order_acquire) && !s.stop.load()) pause_cpu();
}
void child_setup(int cpu) {
  const auto parent = getppid();
  if (prctl(PR_SET_PDEATHSIG, SIGKILL) || getppid() != parent)
    throw std::runtime_error("parent changed");
  pin(cpu);
}

template <salias::Mode M>
void produce(const Options& o, const std::string& name, unsigned id, Shared& s) {
  child_setup(static_cast<int>(o.cpu_base + o.subscribers + id));
  const Schedule plan(o);
  auto connected = salias::Channel<M>::connect(name);
  if (!connected) throw std::runtime_error("publisher connect failed");
  auto channel = std::move(connected).value();
  auto publisher = channel.publisher();
  auto& stats = s.publishers[id];
  ready(s);
  stats.resources.begin();
  const auto end = s.start_ns + plan.duration_ns;
  std::uint64_t seq = 0;
  while (seq < plan.total() && !s.stop.load(std::memory_order_relaxed)) {
    const auto due = s.start_ns + plan.due(seq);
    auto now = now_ns();
    while (now < due && !s.stop.load(std::memory_order_relaxed)) {
      pause_cpu();
      now = now_ns();
    }
    if (now >= end || s.stop.load(std::memory_order_relaxed)) break;
    const auto attempt = now_ns();  // Before the first try_claim, not after success.
    const bool sample = sampled(seq, id);
    std::uint64_t bp_start = 0;
    bool committed = false;
    while (!s.stop.load(std::memory_order_relaxed)) {
      const bool diagnostic = o.diagnostics && sample;
      const auto call_start = diagnostic ? now_ns() : 0;
      auto result = publisher.try_claim(o.payload);
      const auto call_end = diagnostic ? now_ns() : 0;
      if (result) {
        if (diagnostic) stats.successful_claim.record(call_end - call_start);
        auto claim = std::move(result).value();
        const Header h{magic, seq, due, sample ? attempt : 0, id, o.payload, 0};
        auto header = h;
        header.check = check_header(header);
        auto payload = claim.payload();
        std::memcpy(payload.data(), &header, sizeof(header));
        for (unsigned word = sizeof(Header) / 8; word < o.payload / 8; ++word) {
          const auto value = magic ^ seq ^ (std::uint64_t{id} << 61) ^ word;
          std::memcpy(payload.data() + word * 8, &value, sizeof(value));
        }
        const auto commit_start = diagnostic ? now_ns() : 0;
        claim.commit();
        if (diagnostic) stats.commit_call.record(now_ns() - commit_start);
        if (sample || bp_start) now = now_ns();
        if (sample) {
          stats.claim_commit.record(now - attempt);
          stats.schedule_lag.record(attempt - due);
        }
        committed = true;
        break;
      }
      if (result.error() != salias::Error::BackPressured)
        throw std::runtime_error("non-backpressure publisher error");
      if (diagnostic) stats.failed_claim.record(call_end - call_start);
      ++stats.retries;
      now = now_ns();
      if (!bp_start) { bp_start = now; ++stats.bp_messages; }
      if ((stats.retries & 1023) == 0) {
        const auto arrived = plan.arrivals(now - s.start_ns);
        stats.backlog_highwater_observed = std::max(stats.backlog_highwater_observed, arrived - seq);
      }
      if (now >= end) break;
      pause_cpu();
    }
    if (bp_start) {
      const auto blocked = now - bp_start;
      stats.bp_ns += blocked;
      stats.bp_max_ns = std::max(stats.bp_max_ns, blocked);
    }
    if (!committed) break;
    ++seq;
    if ((seq & 255) == 0) {
      stats.sent.store(seq, std::memory_order_relaxed);
      const auto arrived = plan.arrivals(now_ns() - s.start_ns);
      const auto backlog = arrived > seq ? arrived - seq : 0;
      stats.backlog_highwater_observed = std::max(stats.backlog_highwater_observed, backlog);
    }
  }
  stats.sent.store(seq, std::memory_order_release);
  stats.ended_ns = now_ns();
  stats.resources.end();
  stats.done.store(true, std::memory_order_release);
}

template <salias::Mode M>
void consume(const Options& o, const std::string& name, unsigned id, Shared& s) {
  child_setup(static_cast<int>(o.cpu_base + id));
  const Schedule plan(o);
  std::array<std::vector<std::uint64_t>, 2> seen;
  for (auto& bits : seen) bits.resize((plan.total() + 63) / 64);
  auto connected = salias::Channel<M>::connect(name);
  if (!connected) throw std::runtime_error("subscriber connect failed");
  auto channel = std::move(connected).value();
  auto subscriber = channel.subscriber();  // Reliable FIFO only.
  auto& stats = s.subscribers[id];
  ready(s);
  stats.resources.begin();
  const auto end = s.start_ns + plan.duration_ns;
  const auto drain_end = end + 5 * second;
  std::uint64_t count = 0;
  bool cutoff_recorded = false;
  if (id == 0 && o.stall_subscriber_ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(o.stall_subscriber_ms));
  }
  auto handler = [&](const salias::Message& message) noexcept {
    ++count;
    Header h{};
    if (message.payload.size() != o.payload) { ++stats.invalid_publisher; return; }
    std::memcpy(&h, message.payload.data(), sizeof(h));
    const auto receive = h.attempt_ns ? now_ns() : 0;  // Before validation work.
    if (h.publisher >= 2) { ++stats.invalid_publisher; return; }
    auto& seq = stats.sequence[h.publisher];
    ++seq.received;
    if (h.marker != magic || h.bytes != o.payload || h.check != check_header(h)) ++seq.corrupt;
    for (unsigned word = sizeof(Header) / 8; word < o.payload / 8; ++word) {
      std::uint64_t value = 0;
      std::memcpy(&value, message.payload.data() + word * 8, sizeof(value));
      if (value != (magic ^ h.sequence ^ (std::uint64_t{h.publisher} << 61) ^ word)) ++seq.corrupt;
    }
    if (seq.channel_producer_id < 0) seq.channel_producer_id = static_cast<int>(message.producer_id);
    if (seq.channel_producer_id != static_cast<int>(message.producer_id)) ++seq.invalid;
    if (h.sequence >= plan.total()) { ++seq.invalid; return; }
    if (h.planned_ns != s.start_ns + plan.due(h.sequence) ||
        (h.attempt_ns != 0) != sampled(h.sequence, h.publisher)) ++seq.corrupt;
    observe_sequence(seq, seen[h.publisher], h.sequence);
    if (h.attempt_ns) {
      if (receive < h.attempt_ns || receive < h.planned_ns) ++stats.clock_reversals;
      else {
        stats.from_attempt.record(receive - h.attempt_ns);
        stats.from_schedule.record(receive - h.planned_ns);
      }
    }
  };
  while (!s.stop.load(std::memory_order_relaxed)) {
    static_cast<void>(subscriber.poll(64, handler));
    stats.received.store(count, std::memory_order_relaxed);
    const auto now = now_ns();
    if (now >= end && !cutoff_recorded) {
      const auto arrivals = 2 * plan.total();
      stats.unreceived_at_measurement_cutoff = arrivals > count ? arrivals - count : 0;
      cutoff_recorded = true;
    }
    if (now >= end && s.publishers[0].done.load(std::memory_order_acquire) &&
        s.publishers[1].done.load(std::memory_order_acquire)) {
      const auto sent = s.publishers[0].sent.load() + s.publishers[1].sent.load();
      if (count >= sent || now >= drain_end) break;
    }
    if (now >= drain_end) break;
    if ((count & 255) == 0) pause_cpu();
  }
  stats.received.store(count, std::memory_order_release);
  stats.ended_ns = now_ns();
  stats.resources.end();
}

void self_test() {
  Histogram h;
  for (std::uint64_t i = 1; i <= 1000; ++i) h.record(i);
  if (h.samples != 1000 || lat::percentile(h.buckets.data(), h.samples, 99, 100) < 990)
    throw std::runtime_error("histogram percentile self-test failed");
  for (auto v : {0ULL, 63ULL, 64ULL, 65ULL, 49'999ULL, 50'000ULL, 50'001ULL,
                1'000'000ULL, 1'000'000'000'000ULL}) {
    const auto index = lat::bucket_index(v);
    if (index >= lat::kBucketCount || lat::bucket_upper_bound_ns(index) < v)
      throw std::runtime_error("histogram bounds self-test failed");
  }
  Histogram threshold;
  for (int i = 0; i < 999; ++i) threshold.record(50'000);
  threshold.record(50'001);
  if (threshold.above_budget != 1 || !threshold.passes(999, 1000))
    throw std::runtime_error("threshold self-test failed");
  threshold.record(50'001);
  if (threshold.passes(999, 1000)) throw std::runtime_error("threshold rank self-test failed");
  Histogram merged;
  merged.merge(h);
  merged.merge(threshold);
  if (merged.samples != 2001 || merged.above_budget != 2 ||
      std::accumulate(merged.buckets.begin(), merged.buckets.end(), std::uint64_t{0}) != merged.samples)
    throw std::runtime_error("histogram merge self-test failed");
  SequenceStats sequence;
  std::vector<std::uint64_t> bits(1);
  for (auto value : {0, 2, 1, 1, 3}) observe_sequence(sequence, bits, value);
  if (sequence.unique != 4 || sequence.duplicate != 1 || sequence.out_of_order != 1 ||
      sequence.gap_events != 1 || sequence.gap_positions_observed != 1 || sequence.high_sequence_next != 4)
    throw std::runtime_error("sequence fault-detection self-test failed");
  for (bool burst : {false, true}) {
    Options o;
    o.seconds = 2;
    o.burst = burst;
    const Schedule plan(o);
    std::uint64_t previous = 0;
    for (std::uint64_t i = 0; i < plan.total(); ++i) {
      const auto due = plan.due(i);
      if ((i && due <= previous) || plan.arrivals(due) != i + 1 ||
          (due && plan.arrivals(due - 1) != i)) throw std::runtime_error("schedule self-test failed");
      previous = due;
    }
  }
  std::vector<std::uint64_t> deltas;
  deltas.reserve(100'000);
  for (unsigned i = 0; i < 100'000; ++i) {
    const auto a = now_ns(), b = now_ns();
    if (!a || b < a) throw std::runtime_error("clock self-test failed");
    deltas.push_back(b - a);
  }
  std::sort(deltas.begin(), deltas.end());
  struct ClockShared { std::atomic<unsigned> token{0}; std::uint64_t stamp = 0; };
  auto* memory = mmap(nullptr, sizeof(ClockShared), PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (memory == MAP_FAILED) throw std::runtime_error("clock mmap failed");
  auto* c = new (memory) ClockShared;
  const auto pid = fork();
  if (pid < 0) throw std::runtime_error("clock fork failed");
  if (!pid) {
    prctl(PR_SET_PDEATHSIG, SIGKILL);
    for (unsigned i = 0; i < 10'000; ++i) {
      while (c->token.load(std::memory_order_acquire) != 1) pause_cpu();
      const auto t = now_ns();
      if (t < c->stamp) _exit(1);
      c->stamp = t;
      c->token.store(2, std::memory_order_release);
    }
    _exit(0);
  }
  bool failed = false;
  const auto deadline = now_ns() + 10 * second;
  for (unsigned i = 0; i < 10'000; ++i) {
    c->stamp = now_ns();
    c->token.store(1, std::memory_order_release);
    while (c->token.load(std::memory_order_acquire) != 2 && now_ns() < deadline) pause_cpu();
    if (now_ns() >= deadline || now_ns() < c->stamp) { failed = true; break; }
    c->token.store(0, std::memory_order_release);
  }
  if (failed) kill(pid, SIGKILL);
  int status = 0;
  waitpid(pid, &status, 0);
  munmap(c, sizeof(ClockShared));
  if (failed || !WIFEXITED(status) || WEXITSTATUS(status))
    throw std::runtime_error("cross-process CLOCK_MONOTONIC self-test failed");
  std::cout << "{\"type\":\"self_test\",\"pass\":true,\"clock\":\"CLOCK_MONOTONIC\","
            << "\"clock_pair_delta_ns\":{\"min\":" << deltas.front()
            << ",\"p50\":" << deltas[50'000] << ",\"p99\":" << deltas[99'000]
            << ",\"max\":" << deltas.back()
            << "},\"cross_process_clock_roundtrips\":10000,\"histogram_buckets\":"
            << lat::kBucketCount << "}\n";
}

template <salias::Mode M>
bool run(const Options& o) {
  const Schedule plan(o);
  const std::string name = "salias-hft-accept-" + std::to_string(getpid());
  salias::Config config;
  config.name = name;
  config.mode = M;
  config.capacity = 4 * 1024 * 1024;
  config.publication_window = 128 * 1024;
  config.num_producers = 2;
  config.num_consumers = o.subscribers;
  auto created = salias::Channel<M>::create(config);
  if (!created) throw std::runtime_error("channel creation failed");
  auto owner = std::move(created).value();
  void* memory = mmap(nullptr, sizeof(Shared), PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (memory == MAP_FAILED) throw std::runtime_error("statistics mmap failed");
  auto& s = *new (memory) Shared;
  std::vector<pid_t> children;
  std::vector<int> statuses;
  try {
    for (unsigned worker = 0; worker < o.subscribers + 2; ++worker) {
      const auto pid = fork();
      if (pid < 0) throw std::runtime_error("worker fork failed");
      if (!pid) {
        try {
          if (worker < o.subscribers) consume<M>(o, name, worker, s);
          else produce<M>(o, name, worker - o.subscribers, s);
          _exit(0);
        } catch (const std::exception& e) {
          std::cerr << "worker " << worker << ": " << e.what() << '\n';
          s.failed.store(true);
          s.stop.store(true);
          _exit(1);
        }
      }
      children.push_back(pid);
    }
    const auto ready_deadline = now_ns() + 20 * second;
    while (s.ready.load(std::memory_order_acquire) != o.subscribers + 2) {
      if (now_ns() > ready_deadline || s.failed.load() || interrupted)
        throw std::runtime_error("worker readiness failed");
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    s.start_ns = now_ns() + 100'000'000;
    s.start.store(true, std::memory_order_release);
    std::cout << "{\"type\":\"start\",\"start_monotonic_ns\":" << s.start_ns
              << ",\"duration_s\":" << o.seconds << ",\"subscribers\":" << o.subscribers
              << ",\"payload_bytes\":" << o.payload << ",\"pattern\":\""
              << (o.burst ? "burst" : "steady") << "\",\"steady_total_rate\":" << o.rate
              << ",\"peak_total_rate\":" << o.peak_rate << ",\"burst_ms_per_second\":"
              << (o.burst ? o.burst_ms : 0) << ",\"planned_published\":" << plan.total() * 2
              << ",\"capacity_per_publisher_bytes\":4194304,\"publication_window_bytes\":131072,"
              << "\"batch\":1,\"poll_limit\":64,\"sample_hash_mask\":63,\"cpus\":[";
    for (unsigned i = 0; i < o.subscribers + 2; ++i) {
      if (i) std::cout << ',';
      std::cout << i + o.cpu_base;
    }
    std::cout << "],\"diagnostics\":" << (o.diagnostics ? "true" : "false")
              << ",\"stall_subscriber_ms\":" << o.stall_subscriber_ms << "}\n" << std::flush;
    auto next_log = s.start_ns + second;
    std::size_t alive = children.size();
    statuses.resize(children.size(), -1);
    while (alive) {
      const auto now = now_ns();
      if (interrupted || now > s.start_ns + plan.duration_ns + 10 * second) {
        s.stop.store(true);
        s.failed.store(true);
        for (std::size_t i = 0; i < children.size(); ++i)
          if (statuses[i] < 0) kill(children[i], SIGKILL);
      }
      if (now >= next_log) {
        std::cout << "{\"type\":\"progress\",\"elapsed_s\":"
                  << double(now - s.start_ns) / second << ",\"due\":"
                  << plan.arrivals(now - s.start_ns) * 2 << ",\"sent\":["
                  << s.publishers[0].sent.load() << ',' << s.publishers[1].sent.load()
                  << "],\"received\":[";
        for (unsigned i = 0; i < o.subscribers; ++i) {
          if (i) std::cout << ',';
          std::cout << s.subscribers[i].received.load();
        }
        std::cout << "]}\n" << std::flush;
        next_log += second;
      }
      for (std::size_t i = 0; i < children.size(); ++i) {
        if (statuses[i] >= 0) continue;
        int status = 0;
        const auto waited = waitpid(children[i], &status, WNOHANG);
        if (waited == children[i]) {
          statuses[i] = status;
          --alive;
          if (!WIFEXITED(status) || WEXITSTATUS(status)) { s.failed.store(true); s.stop.store(true); }
        }
      }
      if (alive) std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const auto completed = now_ns();
    bool integrity = !s.failed.load();
    std::uint64_t total_sent = 0;
    for (const auto& p : s.publishers) total_sent += p.sent.load();
    const auto unsent = plan.total() * 2 - total_sent;
    Histogram aggregate_attempt, aggregate_schedule;
    std::cout << "{\"type\":\"result\",\"measurement_seconds\":" << o.seconds
              << ",\"wall_seconds_including_drain_and_reap\":" << double(completed - s.start_ns) / second
              << ",\"planned_published\":" << plan.total() * 2 << ",\"published\":" << total_sent
              << ",\"unsent_at_cutoff\":" << unsent << ",\"actual_published_msg_s\":"
              << double(total_sent) / o.seconds << ",\"publishers\":[";
    for (unsigned i = 0; i < 2; ++i) {
      if (i) std::cout << ',';
      const auto& p = s.publishers[i];
      std::cout << "{\"publisher\":" << i << ",\"sent\":" << p.sent.load()
                << ",\"unsent\":" << plan.total() - p.sent.load() << ",\"backpressure_retries\":"
                << p.retries << ",\"backpressured_messages\":" << p.bp_messages
                << ",\"backpressure_episode_ns\":" << p.bp_ns << ",\"max_backpressure_episode_ns\":"
                << p.bp_max_ns << ",\"unsent_backlog_highwater_observed\":" << p.backlog_highwater_observed
                << ",\"first_attempt_to_commit\":";
      p.claim_commit.print();
      std::cout << ",\"planned_to_first_attempt\":";
      p.schedule_lag.print();
      std::cout << ",\"successful_claim\":";
      p.successful_claim.print();
      std::cout << ",\"failed_claim\":";
      p.failed_claim.print();
      std::cout << ",\"commit_call\":";
      p.commit_call.print();
      std::cout << ",\"resources\":";
      p.resources.print();
      std::cout << '}';
    }
    std::cout << "],\"subscribers\":[";
    std::uint64_t delivered = 0;
    for (unsigned i = 0; i < o.subscribers; ++i) {
      if (i) std::cout << ',';
      const auto& sub = s.subscribers[i];
      delivered += sub.received.load();
      const auto expected_samples = s.publishers[0].claim_commit.samples + s.publishers[1].claim_commit.samples;
      bool sub_ok = !sub.invalid_publisher && !sub.clock_reversals &&
                    sub.from_attempt.samples == expected_samples &&
                    sub.from_schedule.samples == expected_samples;
      std::cout << "{\"subscriber\":" << i << ",\"received\":" << sub.received.load()
                << ",\"unreceived_at_measurement_cutoff_observed\":" << sub.unreceived_at_measurement_cutoff
                << ",\"unreceived_after_drain\":" << plan.total() * 2 - std::min(plan.total() * 2, sub.received.load())
                << ",\"invalid_publisher\":" << sub.invalid_publisher
                << ",\"clock_reversals\":" << sub.clock_reversals << ",\"sequence\":[";
      for (unsigned pub = 0; pub < 2; ++pub) {
        if (pub) std::cout << ',';
        const auto& q = sub.sequence[pub];
        const auto sent = s.publishers[pub].sent.load();
        const auto missing = sent > q.unique ? sent - q.unique : 0;
        sub_ok = sub_ok && q.unique == sent && !q.duplicate && !q.out_of_order && !q.gap_events &&
                 !q.corrupt && !q.invalid && q.high_sequence_next == sent;
        std::cout << "{\"publisher\":" << pub << ",\"received\":" << q.received
                  << ",\"unique\":" << q.unique << ",\"missing_published\":" << missing
                  << ",\"duplicates\":" << q.duplicate << ",\"out_of_order\":" << q.out_of_order
                  << ",\"gap_events\":" << q.gap_events << ",\"gap_positions_observed\":"
                  << q.gap_positions_observed << ",\"corrupt\":" << q.corrupt << ",\"invalid\":"
                  << q.invalid << ",\"channel_producer_id\":" << q.channel_producer_id << '}';
      }
      sub_ok = sub_ok && sub.sequence[0].channel_producer_id != sub.sequence[1].channel_producer_id;
      integrity = integrity && sub_ok;
      std::cout << "],\"integrity_pass\":" << (sub_ok ? "true" : "false") << ",\"from_first_attempt\":";
      sub.from_attempt.print();
      std::cout << ",\"from_planned_arrival\":";
      sub.from_schedule.print();
      std::cout << ",\"resources\":";
      sub.resources.print();
      std::cout << '}';
      aggregate_attempt.merge(sub.from_attempt);
      aggregate_schedule.merge(sub.from_schedule);
    }
    std::cout << "],\"aggregate_from_first_attempt\":";
    aggregate_attempt.print();
    std::cout << ",\"aggregate_from_planned_arrival\":";
    aggregate_schedule.print();
    std::cout << ",\"delivered\":" << delivered << ",\"integrity_pass\":" << (integrity ? "true" : "false")
              << ",\"offered_load_completed\":" << (!unsent && delivered == total_sent * o.subscribers ? "true" : "false")
              << ",\"worker_failure\":" << (s.failed.load() ? "true" : "false") << "}\n" << std::flush;
    s.~Shared();
    munmap(memory, sizeof(Shared));
    return integrity;
  } catch (...) {
    s.stop.store(true);
    for (std::size_t i = 0; i < children.size(); ++i) {
      if (statuses.size() > i && statuses[i] >= 0) continue;
      kill(children[i], SIGKILL);
      waitpid(children[i], nullptr, 0);
    }
    s.~Shared();
    munmap(memory, sizeof(Shared));
    throw;
  }
}
}  // namespace

int main(int argc, char** argv) {
  try {
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    Options o;
    for (int i = 1; i < argc; ++i) {
      const std::string key = argv[i];
      if (key == "--self-test") { self_test(); return 0; }
      if (key == "--diagnostics") { o.diagnostics = true; continue; }
      if (i + 1 == argc) throw std::runtime_error("missing argument value");
      const std::string value = argv[++i];
      if (key == "--subscribers") o.subscribers = std::stoul(value);
      else if (key == "--payload") o.payload = std::stoul(value);
      else if (key == "--seconds") o.seconds = std::stoul(value);
      else if (key == "--cpu-base") o.cpu_base = std::stoul(value);
      else if (key == "--rate") o.rate = std::stoull(value);
      else if (key == "--peak-rate") o.peak_rate = std::stoull(value);
      else if (key == "--burst-ms") o.burst_ms = std::stoul(value);
      else if (key == "--stall-subscriber-ms") o.stall_subscriber_ms = std::stoul(value);
      else if (key == "--pattern" && (value == "steady" || value == "burst")) o.burst = value == "burst";
      else throw std::runtime_error("unknown argument: " + key);
    }
    // Whole-millisecond rates keep the inverse arrival schedule exact.
    if (o.subscribers < 1 || o.subscribers > 2 || (o.payload != 64 && o.payload != 256) ||
        !o.seconds || !o.rate || !o.peak_rate || o.rate % 2000 || o.peak_rate % 2000 ||
        o.burst_ms >= 1000 || o.cpu_base + o.subscribers + 2 > CPU_SETSIZE)
      throw std::runtime_error("invalid options; rates must be positive multiples of 2000");
    return o.subscribers == 1 ? (run<salias::Mode::FifoMpsc>(o) ? 0 : 2)
                              : (run<salias::Mode::FifoMpmc>(o) ? 0 : 2);
  } catch (const std::exception& e) {
    std::cerr << "acceptance: " << e.what() << '\n';
    return 1;
  }
}
