#include <iostream>
#include <utility>

#include "common.hpp"

int main(int argc, char** argv) {
  salias_example::Options options{
      .name = "salias-demo-mpsc",
      .message = "",
  };
  const salias_example::ParseSpec spec{.allow_capacity = true};
  const auto parsed = salias_example::parse_args(argc, argv, options, spec, std::cerr);
  if (parsed == salias_example::ParseStatus::Help) {
    salias_example::print_usage(std::cout, argv[0], spec);
    return 0;
  }
  if (parsed == salias_example::ParseStatus::Error) {
    salias_example::print_usage(std::cerr, argv[0], spec);
    return 2;
  }

  salias::Config config;
  config.name = options.name;
  config.mode = salias::Mode::FifoMpsc;
  config.capacity = options.capacity;

  auto created = salias::FifoMpscChannel::create(config);
  if (!created) {
    std::cerr << "failed to create MPSC channel: " << salias_example::error_name(created.error())
              << '\n';
    return 1;
  }

  auto channel = std::move(created).value();
  auto subscriber = channel.subscriber();
  std::cout << "mpsc subscriber ready name=" << options.name << " count=" << options.count << '\n';
  std::cout.flush();
  salias_example::receive_messages(subscriber, options.count, "mpsc");
  return 0;
}
