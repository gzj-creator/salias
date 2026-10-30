/**
 * @file test/api/schema_helper_test.cpp
 * @brief 验证显式 Schema 辅助模板的编译期契约和 typed Channel 收发。
 */
#include <unistd.h>

#include <cstdint>
#include <salias/salias.hpp>
#include <string>
#include <type_traits>

namespace {

struct Quote {
  std::uint64_t sequence;
};

using QuoteSchema = salias::Schema<Quote, 0x534348454D413031ull, 3>;

static_assert(std::is_same_v<QuoteSchema::message_type, Quote>);
static_assert(QuoteSchema::schema_id == 0x534348454D413031ull);
static_assert(QuoteSchema::version == 3);
static_assert(std::is_empty_v<QuoteSchema>);

}  // namespace

int main() {
  salias::Config config;
  config.name = "salias-schema-helper-" + std::to_string(::getpid());

  using Channel = salias::Channel<salias::Mode::FifoMpsc, QuoteSchema>;
  auto created = Channel::create(config);
  if (!created) {
    return 1;
  }

  auto publisher = created->publisher();
  auto subscriber = created->subscriber();
  if (!publisher.send(Quote{.sequence = 42})) {
    return 2;
  }

  auto received = subscriber.try_recv_copy();
  if (!received || !*received || (**received).sequence != 42) {
    return 3;
  }
  return 0;
}
