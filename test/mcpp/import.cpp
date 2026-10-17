import std;
import salias;

bool header_publish(const salias::Config& config);
bool header_latest_by_key_publish(std::string_view name);
bool header_priority_publish(std::string_view high_name, std::string_view low_name);

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

    auto expired = publisher.offer_for(payload, std::chrono::nanoseconds{0});
    require(expired && *expired);
    require(!subscriber.try_recv());
    require(subscriber.expired_messages() == 1);

    auto ttl_claim = publisher.try_claim_for(payload.size(), std::chrono::seconds{1});
    require(ttl_claim.has_value());
    std::ranges::copy(payload, ttl_claim->payload().begin());
    ttl_claim->commit();
    auto ttl_message = subscriber.try_recv();
    require(ttl_message && ttl_message->expires_at_monotonic_ns > 0);
    subscriber.release(*ttl_message);

    auto ttl_batch = publisher.offer_batch_for(batch, std::chrono::seconds{1});
    require(ttl_batch && *ttl_batch == batch.size());
    received = 0;
    const auto ttl_polled = subscriber.poll(8, [&](const salias::Message& item) noexcept {
      valid =
          valid && std::ranges::equal(item.payload, payload) && item.expires_at_monotonic_ns > 0;
      ++received;
    });
    require(valid && ttl_polled == 2 && received == 2);
  }
  const auto after_cleanup = salias::Channel<M>::connect(config.name);
  require(!after_cleanup && after_cleanup.error() == salias::Error::NotFound);
}
}  // namespace

int main() {
  try {
    static_assert(std::is_same_v<salias::FifoMpscChannel, salias::Channel<salias::Mode::FifoMpsc>>);
    static_assert(std::is_same_v<salias::OrderedMpmcSubscriber,
                                 salias::Subscriber<salias::Mode::OrderedMpmc>>);
    roundtrip<salias::Mode::FifoMpsc>();
    roundtrip<salias::Mode::FifoMpmc>();
    roundtrip<salias::Mode::OrderedMpsc>();
    roundtrip<salias::Mode::OrderedMpmc>();
    roundtrip<salias::Mode::LatestMpmc>();

    salias::Config window_config;
    window_config.name =
        "salias-mcpp-message-window-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    window_config.mode = salias::Mode::FifoMpsc;
    window_config.capacity = 4096;
    window_config.publication_window_messages = 1;
    auto window_channel = salias::FifoMpscChannel::create(window_config);
    require(window_channel.has_value());
    auto window_publisher = window_channel->publisher();
    auto window_subscriber = window_channel->subscriber();
    const std::array window_payload{std::byte{7}};
    auto first_window_offer = window_publisher.offer(window_payload);
    require(first_window_offer && *first_window_offer);
    auto window_full = window_publisher.offer(window_payload);
    require(!window_full && window_full.error() == salias::Error::BackPressured);
    auto window_message = window_subscriber.try_recv();
    require(window_message.has_value());
    window_subscriber.release(*window_message);
    auto resumed_window_offer = window_publisher.offer(window_payload);
    require(resumed_window_offer && *resumed_window_offer);

    salias::Config latest_config;
    latest_config.name =
        "salias-mcpp-latest-typed-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    latest_config.mode = salias::Mode::LatestMpmc;
    latest_config.capacity = 8192;
    latest_config.num_consumers = 2;
    using LatestChannel = salias::LatestMpmcChannel<4096, 75>;
    auto latest = LatestChannel::create(latest_config);
    require(latest.has_value());
    require(latest->capacity() == 4096 && latest->high_watermark_percent() == 75);
    auto latest_publisher = latest->publisher();
    auto latest_reliable = latest->subscriber(salias::SubscriptionPolicy::Reliable);
    auto latest_subscriber_result = latest->try_subscriber(salias::SubscriptionPolicy::DropOldest);
    require(latest_subscriber_result.has_value());
    auto latest_subscriber = std::move(*latest_subscriber_result);
    const std::array latest_payload{std::byte{5}, std::byte{6}};
    require(latest_publisher.offer(latest_payload).has_value());
    auto reliable_message = latest_reliable.try_recv_checked();
    require(reliable_message && *reliable_message &&
            std::ranges::equal((**reliable_message).payload, latest_payload));
    latest_reliable.release(**reliable_message);
    auto latest_message = latest_subscriber.try_recv();
    require(latest_message && std::ranges::equal(latest_message->payload, latest_payload));
    latest_subscriber.release(*latest_message);
    latest_reliable = {};

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

    using LatestByKey = salias::LatestByKeyChannel<std::uint64_t, std::uint64_t, 4>;
    static_assert(std::is_same_v<LatestByKey::Publisher,
                                 salias::LatestByKeyPublisher<std::uint64_t, std::uint64_t>>);
    static_assert(std::is_same_v<LatestByKey::Subscriber,
                                 salias::LatestByKeySubscriber<std::uint64_t, std::uint64_t, 4>>);
    static_assert(std::is_same_v<LatestByKey::Message,
                                 salias::LatestByKeyMessage<std::uint64_t, std::uint64_t>>);
    const auto latest_by_key_name =
        "salias-mcpp-latest-by-key-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    auto latest_by_key = LatestByKey::create(latest_by_key_name);
    require(latest_by_key.has_value());
    auto latest_by_key_publisher = latest_by_key->publisher();
    require(latest_by_key_publisher.offer(7, 10).has_value());
    require(latest_by_key_publisher.offer(7, 11).has_value());
    auto latest_by_key_subscriber = latest_by_key->subscriber();
    auto latest_by_key_message = latest_by_key_subscriber.try_recv_checked();
    require(latest_by_key_message && *latest_by_key_message && (**latest_by_key_message).key == 7 &&
            (**latest_by_key_message).value == 11 && (**latest_by_key_message).version == 2);
    require(!latest_by_key_subscriber.try_recv());
    require(header_latest_by_key_publish(latest_by_key_name));
    auto latest_by_key_header_message = latest_by_key_subscriber.try_recv_checked();
    require(latest_by_key_header_message && *latest_by_key_header_message &&
            (**latest_by_key_header_message).value == 12 &&
            (**latest_by_key_header_message).version == 3);
    auto latest_by_key_connected = LatestByKey::connect(latest_by_key_name);
    require(latest_by_key_connected.has_value());

    using VersionedLatestByKey = salias::LatestByKeyChannel<std::uint64_t, std::uint64_t, 4, 1>;
    const auto versioned_latest_by_key_name = latest_by_key_name + "-v1";
    auto versioned_latest_by_key = VersionedLatestByKey::create(versioned_latest_by_key_name);
    require(versioned_latest_by_key.has_value());
    require(VersionedLatestByKey::connect(versioned_latest_by_key_name).has_value());

    static_assert(salias::PriorityPublisher<salias::Mode::FifoMpsc,
                                            salias::Mode::FifoMpsc>::priority_count() == 2);
    static_assert(salias::PrioritySubscriber<salias::Mode::FifoMpsc,
                                             salias::Mode::FifoMpsc>::priority_count() == 2);
    const auto priority_suffix =
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    salias::Config high_priority_config;
    high_priority_config.name = "salias-mcpp-priority-high-" + priority_suffix;
    high_priority_config.capacity = 4096;
    high_priority_config.num_producers = 2;
    salias::Config low_priority_config;
    low_priority_config.name = "salias-mcpp-priority-low-" + priority_suffix;
    low_priority_config.capacity = 4096;
    low_priority_config.num_producers = 2;
    auto high_priority = salias::FifoMpscChannel::create(high_priority_config);
    auto low_priority = salias::FifoMpscChannel::create(low_priority_config);
    require(high_priority && low_priority);
    salias::PrioritySubscriber<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc> priority_subscriber(
        high_priority->subscriber(), low_priority->subscriber());
    salias::PriorityPublisher<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc> priority_publisher(
        high_priority->publisher(), low_priority->publisher());
    const std::array high_priority_payload{std::byte{1}, std::byte{2}};
    require(priority_publisher.offer<0>(high_priority_payload).has_value());
    auto high_priority_message = priority_subscriber.try_recv_checked();
    require(high_priority_message && *high_priority_message &&
            (**high_priority_message).priority == 0);
    require(priority_subscriber.release(**high_priority_message).has_value());
    require(header_priority_publish(high_priority_config.name, low_priority_config.name));
    auto low_priority_message = priority_subscriber.try_recv_checked();
    require(low_priority_message && *low_priority_message &&
            (**low_priority_message).priority == 1 &&
            std::ranges::equal((**low_priority_message).message.payload,
                               std::array{std::byte{9}, std::byte{8}}));
    require(priority_subscriber.release(**low_priority_message).has_value());

    std::cout << "Module/header IPC, priority, latest-by-key, TTL, message windows, claim, batch, "
                 "poll, checked "
                 "lag and seek passed for 5 modes\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
