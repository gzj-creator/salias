#include <iostream>
#include <string>
#include <utility>

#include "common.hpp"

int main(int argc, char** argv) {
  salias_example::Options options{
      .name = "salias-demo-mpsc",
      .message = "hello from mpsc publisher",
  };
  const salias_example::ParseSpec spec{.allow_message = true};
  const auto parsed = salias_example::parse_args(argc, argv, options, spec, std::cerr);
  if (parsed == salias_example::ParseStatus::Help) {
    salias_example::print_usage(std::cout, argv[0], spec);
    return 0;
  }
  if (parsed == salias_example::ParseStatus::Error) {
    salias_example::print_usage(std::cerr, argv[0], spec);
    return 2;
  }

  auto connected = salias_example::connect_with_retry<salias::Mode::FifoMpsc>(options.name);
  if (!connected) {
    std::cerr << "failed to connect MPSC channel: " << salias_example::error_name(connected.error())
              << '\n';
    return 1;
  }

  auto channel = std::move(connected).value();
  auto publisher = channel.publisher();
  for (std::uint64_t i = 0; i < options.count; ++i) {
    const std::string payload = options.message + " #" + std::to_string(i);
    auto published = salias_example::publish_text(publisher, payload);
    if (!published) {
      std::cerr << "failed to publish MPSC message: "
                << salias_example::error_name(published.error()) << '\n';
      return 1;
    }
  }
  std::cout << "mpsc published count=" << options.count << '\n';
  return 0;
}
