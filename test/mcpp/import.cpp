import std;
import salias;

bool header_publish(const salias::Config& config);

namespace {
void require(bool condition, std::source_location location = std::source_location::current()) {
  if (!condition) {
    throw std::runtime_error("salias module/header IPC check failed at line " +
                             std::to_string(location.line()));
  }
}

template <salias::Mode M>
void roundtrip() {
  salias::Config config;
  config.name = "salias-mcpp-" + std::to_string(static_cast<int>(M)) + "-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  config.mode = M;
  config.capacity = 4096;
  config.num_producers = 2;
  const std::array payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
  {
    salias::Result<salias::Channel<M>> owner = salias::Channel<M>::create(config);
    require(owner.has_value());
    auto subscriber_result = owner->try_subscriber();
    require(subscriber_result.has_value());
    auto exhausted = owner->try_subscriber();
    require(!exhausted && exhausted.error() == salias::Error::NoSubscriberSlots);
    salias::Subscriber<M> subscriber = std::move(*subscriber_result);
    require(header_publish(config));
    auto message = subscriber.try_recv();
    require(message && std::ranges::equal(message->payload, payload));
    subscriber.release(*message);

    salias::Publisher<M> publisher = owner->publisher();
    auto claim = publisher.try_claim(payload.size());
    require(claim.has_value());
    std::ranges::copy(payload, claim->payload().begin());
    claim->commit();
    const std::array<std::span<const std::byte>, 2> batch{payload, payload};
    const auto offered = publisher.offer_batch(batch);
    require(offered && *offered == batch.size());
    std::size_t received = 0;
    bool valid = true;
    const auto polled = subscriber.poll(8, [&](const salias::Message& item) noexcept {
      valid = valid && std::ranges::equal(item.payload, payload);
      ++received;
    });
    if (!valid || polled != 3 || received != 3) {
      throw std::runtime_error(
          "mode=" + std::to_string(static_cast<int>(M)) + " polled=" + std::to_string(polled) +
          " received=" + std::to_string(received) + " valid=" + std::to_string(valid));
    }
  }
  const auto after_cleanup = salias::Channel<M>::connect(config.name);
  require(!after_cleanup && after_cleanup.error() == salias::Error::NotFound);
}
}  // namespace

int main() {
  try {
    static_assert(std::is_same_v<salias::FifoMpscChannel, salias::Channel<salias::Mode::FifoMpsc>>);
    static_assert(std::is_same_v<salias::OrderedFanoutSubscriber,
                                 salias::Subscriber<salias::Mode::OrderedFanout>>);
    roundtrip<salias::Mode::FifoMpsc>();
    roundtrip<salias::Mode::FifoFanout>();
    roundtrip<salias::Mode::OrderedMpsc>();
    roundtrip<salias::Mode::OrderedFanout>();
    roundtrip<salias::Mode::LatestFanout>();

    salias::Config latest_config;
    latest_config.name =
        "salias-mcpp-latest-typed-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    latest_config.mode = salias::Mode::LatestFanout;
    latest_config.capacity = 8192;
    using LatestChannel = salias::LatestFanoutChannel<4096, 75>;
    auto latest = LatestChannel::create(latest_config);
    require(latest.has_value());
    require(latest->capacity() == 4096 && latest->high_watermark_percent() == 75);
    auto latest_publisher = latest->publisher();
    auto latest_subscriber_result = latest->try_subscriber();
    require(latest_subscriber_result.has_value());
    auto latest_subscriber = std::move(*latest_subscriber_result);
    const std::array latest_payload{std::byte{5}, std::byte{6}};
    require(latest_publisher.offer(latest_payload).has_value());
    auto latest_message = latest_subscriber.try_recv();
    require(latest_message && std::ranges::equal(latest_message->payload, latest_payload));
    latest_subscriber.release(*latest_message);

    for (std::uint32_t sequence = 0; sequence < 300; ++sequence) {
      require(latest_publisher.offer(latest_payload).has_value());
    }
    auto lagged = latest_subscriber.try_recv_checked();
    require(!lagged && lagged.error() == salias::Error::Lagged);
    require(latest_subscriber.lagged_messages() == 108);
    auto skipped = latest_subscriber.skip_to_latest();
    require(skipped && *skipped == 191);
    require(latest_subscriber.skipped_messages() == 191);
    auto checked_latest = latest_subscriber.try_recv_checked();
    require(checked_latest && *checked_latest && (**checked_latest).sequence == 300);
    latest_subscriber.release(**checked_latest);

    std::cout << "Module/header IPC, claim, batch, poll, checked lag and seek passed for 5 modes\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
