import std;
import salias;

#include "typed_schema.hpp"

bool header_publish(const salias::Config& config);
bool header_typed_publish(std::string_view name, const salias_mcpp::TypedPayload& payload);

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
    const salias_mcpp::TypedPayload typed_payload{.id = 7, .sequence = 11};
    require(publisher.send(typed_payload).has_value());
    auto typed_message = subscriber.template try_recv_copy<salias_mcpp::TypedPayload>();
    require(typed_message && *typed_message && (**typed_message).id == typed_payload.id &&
            (**typed_message).sequence == typed_payload.sequence);

    auto claim = publisher.try_claim(payload.size());
    require(claim.has_value());
    std::ranges::copy(payload, claim->payload().begin());
    publisher.commit(std::move(*claim));
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
    publisher.commit(std::move(*ttl_claim));
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
    static_assert(
        std::is_same_v<salias_mcpp::TypedPayloadSchema,
                       salias::Schema<salias_mcpp::TypedPayload, 0x53414C4941534D43ull, 1>>);
    static_assert(std::is_same_v<salias::OrderedMpmcSubscriber,
                                 salias::Subscriber<salias::Mode::OrderedMpmc>>);
    roundtrip<salias::Mode::FifoMpsc>();
    roundtrip<salias::Mode::FifoMpmc>();
    roundtrip<salias::Mode::OrderedMpsc>();
    roundtrip<salias::Mode::OrderedMpmc>();

    using TypedChannel = salias::Channel<salias::Mode::FifoMpsc, salias_mcpp::TypedPayloadSchema>;
    salias::Config typed_config;
    typed_config.name = "salias-mcpp-schema-" +
                        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    typed_config.capacity = 4096;
    auto typed_channel = TypedChannel::create(typed_config);
    require(typed_channel.has_value());
    auto typed_subscriber = typed_channel->subscriber();
    const salias_mcpp::TypedPayload schema_payload{.id = 12, .sequence = 34};
    require(header_typed_publish(typed_config.name, schema_payload));
    auto schema_received = typed_subscriber.try_recv_copy();
    require(schema_received && *schema_received && **schema_received == schema_payload);
    require(TypedChannel::connect(typed_config.name).has_value());
    require(!salias::Channel<salias::Mode::FifoMpsc>::connect(typed_config.name).has_value());

    salias::Config window_config;
    window_config.name =
        "salias-mcpp-message-window-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    window_config.mode = salias::Mode::FifoMpsc;
    window_config.capacity = 4096;
    window_config.publication_window_messages = 1;
    auto window_channel = salias::Channel<salias::Mode::FifoMpsc>::create(window_config);
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

    salias::Config typed_latest_config;
    typed_latest_config.name =
        "salias-mcpp-latest-schema-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    typed_latest_config.mode = salias::Mode::LatestMpmc;
    typed_latest_config.capacity = 8192;
    using TypedLatestChannel = salias::LatestMpmcChannel<4096, 75, salias_mcpp::TypedPayloadSchema>;
    auto typed_latest = TypedLatestChannel::create(typed_latest_config);
    require(typed_latest.has_value());
    auto typed_latest_publisher = typed_latest->publisher();
    auto typed_latest_subscriber = typed_latest->subscriber();
    const salias_mcpp::TypedPayload typed_latest_payload{.id = 21, .sequence = 44};
    require(typed_latest_publisher.send(typed_latest_payload).has_value());
    auto typed_latest_received = typed_latest_subscriber.try_recv_copy();
    require(typed_latest_received && *typed_latest_received &&
            **typed_latest_received == typed_latest_payload);
    auto raw_latest_connect =
        salias::LatestMpmcChannel<4096, 75>::connect(typed_latest_config.name);
    require(!raw_latest_connect && raw_latest_connect.error() == salias::Error::SchemaMismatch);

    std::cout << "Module/header IPC, TTL, message windows, claim, batch, poll, checked lag and "
                 "seek passed for 5 modes\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
