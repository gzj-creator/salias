#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "Aeron.h"
#include "concurrent/BusySpinIdleStrategy.h"
#include "concurrent/logbuffer/BufferClaim.h"

namespace {

struct Options {
  std::string scenario = "spsc";
  std::string dir;
  std::uint64_t messages = 1'000'000;
  std::uint32_t producers = 1;
  std::uint32_t consumers = 1;
  std::int32_t stream_id = 1001;
  std::int32_t payload = 64;
  std::int32_t fragment_limit = 64;
};

// 解析 Aeron IPC 对比工具的命令行参数。
Options parse_options(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto require_value = [&](const char* name) -> std::string {
      if (i + 1 >= argc) {
        std::cerr << "missing value for " << name << '\n';
        std::exit(2);
      }
      return argv[++i];
    };

    if (arg == "--scenario") {
      options.scenario = require_value("--scenario");
    } else if (arg == "--dir") {
      options.dir = require_value("--dir");
    } else if (arg == "--messages") {
      options.messages = std::stoull(require_value("--messages"));
    } else if (arg == "--producers") {
      options.producers = static_cast<std::uint32_t>(std::stoul(require_value("--producers")));
    } else if (arg == "--consumers") {
      options.consumers = static_cast<std::uint32_t>(std::stoul(require_value("--consumers")));
    } else if (arg == "--payload") {
      options.payload = static_cast<std::int32_t>(std::stol(require_value("--payload")));
    } else if (arg == "--stream") {
      options.stream_id = static_cast<std::int32_t>(std::stol(require_value("--stream")));
    } else if (arg == "--fragment-limit") {
      options.fragment_limit =
          static_cast<std::int32_t>(std::stol(require_value("--fragment-limit")));
    } else if (arg == "--help") {
      std::cout << "usage: aeron_ipc_compare --dir DIR --scenario spsc|spmc|mpmc "
                   "[--messages N] [--producers N] [--consumers N] [--payload N]\n";
      std::exit(0);
    } else {
      std::cerr << "unknown argument: " << arg << '\n';
      std::exit(2);
    }
  }

  if (options.dir.empty()) {
    std::cerr << "--dir is required\n";
    std::exit(2);
  }
  return options;
}

// 标记 worker 已就绪，并等待共享 start 标志。
void wait_for_start(std::atomic<std::uint32_t>& ready, std::atomic<bool>& start) {
  ready.fetch_add(1, std::memory_order_release);
  while (!start.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
}

// 等待所有 Aeron publication 和 subscription 完成连接。
bool wait_connected(
    const std::vector<std::shared_ptr<aeron::ExclusivePublication>>& publications,
    const std::vector<std::shared_ptr<aeron::Subscription>>& subscriptions,
    std::uint32_t expected_images) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < deadline) {
    bool ok = true;
    for (const auto& publication : publications) {
      ok = ok && publication && publication->isConnected();
    }
    for (const auto& subscription : subscriptions) {
      ok = ok && subscription && subscription->isConnected() &&
           subscription->imageCount() >= static_cast<int>(expected_images);
    }
    if (ok) {
      return true;
    }
    std::this_thread::yield();
  }
  return false;
}

struct Result {
  std::uint32_t producers = 0;
  std::uint32_t consumers = 0;
  std::uint64_t published = 0;
  std::uint64_t delivered = 0;
  std::int32_t payload = 0;
  double seconds = 0.0;
};

// 按请求的生产者和消费者数量运行一个 Aeron IPC benchmark 场景。
Result run_case(const Options& options) {
  const std::string channel = "aeron:ipc";
  aeron::Context context;
  context.aeronDir(options.dir);
  aeron::Aeron aeron(context);

  std::vector<std::shared_ptr<aeron::Subscription>> subscriptions;
  subscriptions.reserve(options.consumers);
  for (std::uint32_t i = 0; i < options.consumers; ++i) {
    const auto id = aeron.addSubscription(channel, options.stream_id);
    std::shared_ptr<aeron::Subscription> subscription;
    while (!subscription) {
      std::this_thread::yield();
      subscription = aeron.findSubscription(id);
    }
    subscriptions.push_back(subscription);
  }

  std::vector<std::shared_ptr<aeron::ExclusivePublication>> publications;
  publications.reserve(options.producers);
  for (std::uint32_t i = 0; i < options.producers; ++i) {
    const auto id = aeron.addExclusivePublication(channel, options.stream_id);
    std::shared_ptr<aeron::ExclusivePublication> publication;
    while (!publication) {
      std::this_thread::yield();
      publication = aeron.findExclusivePublication(id);
    }
    publications.push_back(publication);
  }

  if (!wait_connected(publications, subscriptions, options.producers)) {
    throw std::runtime_error("Aeron publications/subscriptions did not connect");
  }

  std::atomic<std::uint32_t> ready{0};
  std::atomic<bool> start{false};
  std::atomic<bool> failed{false};
  std::vector<std::uint64_t> consumed(options.consumers, 0);
  std::vector<std::thread> threads;
  threads.reserve(options.producers + options.consumers);
  const std::uint64_t expected_per_consumer =
      static_cast<std::uint64_t>(options.producers) * options.messages;

  for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
    threads.emplace_back([&, consumer] {
      auto subscription = subscriptions[consumer];
      aeron::concurrent::BusySpinIdleStrategy idle;
      auto handler = [&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                         aeron::util::index_t, aeron::Header&) {
        ++consumed[consumer];
      };

      wait_for_start(ready, start);
      while (consumed[consumer] < expected_per_consumer) {
        const int fragments = subscription->poll(handler, options.fragment_limit);
        idle.idle(fragments);
      }
    });
  }

  for (std::uint32_t producer = 0; producer < options.producers; ++producer) {
    threads.emplace_back([&, producer] {
      auto publication = publications[producer];
      aeron::concurrent::BusySpinIdleStrategy idle;
      aeron::concurrent::logbuffer::BufferClaim claim;
      wait_for_start(ready, start);
      for (std::uint64_t i = 0; i < options.messages; ++i) {
        idle.reset();
        for (;;) {
          const auto position = publication->tryClaim(options.payload, claim);
          if (position > 0) {
            claim.buffer().putInt64(claim.offset(), static_cast<std::int64_t>(i));
            claim.commit();
            break;
          }
          if (position == aeron::PUBLICATION_CLOSED ||
              position == aeron::MAX_POSITION_EXCEEDED) {
            failed.store(true, std::memory_order_release);
            return;
          }
          idle.idle();
        }
      }
    });
  }

  while (ready.load(std::memory_order_acquire) != options.consumers + options.producers) {
    std::this_thread::yield();
  }
  const auto begin = std::chrono::steady_clock::now();
  start.store(true, std::memory_order_release);
  for (auto& thread : threads) {
    thread.join();
  }
  const auto end = std::chrono::steady_clock::now();

  if (failed.load(std::memory_order_acquire)) {
    throw std::runtime_error("Aeron publication failed");
  }

  std::uint64_t delivered = 0;
  for (const auto value : consumed) {
    if (value != expected_per_consumer) {
      throw std::runtime_error("Aeron delivery mismatch");
    }
    delivered += value;
  }

  return Result{.producers = options.producers,
                .consumers = options.consumers,
                .published = static_cast<std::uint64_t>(options.producers) * options.messages,
                .delivered = delivered,
                .payload = options.payload,
                .seconds = std::chrono::duration<double>(end - begin).count()};
}

// 打印一行机器可读的 Aeron benchmark 结果。
void print_result(const std::string& scenario, const Result& result) {
  const double publish_rate = static_cast<double>(result.published) / result.seconds;
  const double delivery_rate = static_cast<double>(result.delivered) / result.seconds;
  const double mib_per_second =
      delivery_rate * static_cast<double>(result.payload) / (1024.0 * 1024.0);
  std::cout << "RESULT library=aeron-cpp"
            << " scenario=" << scenario
            << " producers=" << result.producers
            << " consumers=" << result.consumers
            << " payload=" << result.payload
            << " published=" << result.published
            << " delivered=" << result.delivered
            << " seconds=" << result.seconds
            << " publish_msg_per_sec=" << publish_rate
            << " delivery_msg_per_sec=" << delivery_rate
            << " delivery_mib_per_sec=" << mib_per_second << '\n';
}

}  // namespace

// 规整请求的场景，运行 benchmark，并报告失败原因。
int main(int argc, char** argv) {
  try {
    Options options = parse_options(argc, argv);
    if (options.scenario == "spsc") {
      options.producers = 1;
      options.consumers = 1;
    } else if (options.scenario == "spmc") {
      options.producers = 1;
      if (options.consumers == 1) {
        options.consumers = 2;
      }
    } else if (options.scenario == "mpmc") {
      if (options.producers == 1) {
        options.producers = 4;
      }
      if (options.consumers == 1) {
        options.consumers = 2;
      }
    } else {
      std::cerr << "unknown scenario: " << options.scenario << '\n';
      return 2;
    }
    print_result(options.scenario, run_case(options));
  } catch (const std::exception& error) {
    std::cerr << "ERROR " << error.what() << '\n';
    return 1;
  }
  return 0;
}
