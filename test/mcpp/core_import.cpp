import std;
import salias;

int main() {
  try {
    salias::Config config;
    config.name = "salias-mcpp-core-" +
                  std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    config.capacity = 4096;
    auto channel = salias::Channel<salias::Mode::FifoMpsc>::create(config);
    if (!channel) {
      return 1;
    }
    auto publisher = channel->publisher();
    auto subscriber = channel->subscriber();
    const std::array payload{std::byte{1}, std::byte{2}, std::byte{3}};
    const auto offered = publisher.send(payload);
    if (!offered || !*offered) {
      return 2;
    }
    auto message = subscriber.try_recv();
    if (!message || !std::ranges::equal(message->payload, payload)) {
      return 3;
    }
    subscriber.release(*message);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 4;
  }
}
