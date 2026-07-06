#include "core/metrics/counters.hpp"
#include "core/metrics/layout.hpp"
#include "core/metrics/reader.hpp"
#include "core/platform/mapping.hpp"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace {

TEST(MetricsLayoutTest, HeaderAndCounterSlotAreCacheLineSized) {
  static_assert(alignof(salias::metrics::MetaHeader) == 64);
  static_assert(sizeof(salias::metrics::MetaHeader) == 64);
  static_assert(alignof(salias::metrics::CounterSlot) == 64);
  static_assert(sizeof(salias::metrics::CounterSlot) == 64);

  EXPECT_EQ(offsetof(salias::metrics::MetaHeader, magic), 0u);
  EXPECT_EQ(offsetof(salias::metrics::MetaHeader, counter_count), 8u);
  EXPECT_EQ(offsetof(salias::metrics::CounterSlot, value), 0u);
  EXPECT_EQ(offsetof(salias::metrics::CounterSlot, label), 16u);
}

TEST(CountersTest, RejectsRegionTooSmall) {
  std::array<std::byte, sizeof(salias::metrics::MetaHeader)> region{};

  auto counters = salias::metrics::Counters::create_in(region, 1);

  ASSERT_FALSE(counters);
  EXPECT_EQ(counters.error(), salias::metrics::MetricsError::RegionTooSmall);
}

TEST(CountersTest, WriterAndReaderShareValuesAndMetadata) {
  constexpr std::uint32_t kCount = 3;
  std::vector<std::byte> region(salias::metrics::region_size(kCount));

  auto counters_result = salias::metrics::Counters::create_in(region, kCount);
  ASSERT_TRUE(counters_result);
  auto counters = std::move(counters_result).value();

  counters.define(0, salias::metrics::CounterType::ProducerPos, 7, "producer_pos");
  counters.define(1, salias::metrics::CounterType::MessagesPublished, 7, "messages");
  counters.set_release(0, 128);
  counters.incr(1);
  counters.incr(1, 4);

  auto reader_result = salias::metrics::CountersReader::view(region);
  ASSERT_TRUE(reader_result);
  auto reader = reader_result.value();

  ASSERT_TRUE(reader.valid());
  ASSERT_EQ(reader.slots().size(), kCount);
  EXPECT_EQ(reader.value(0), 128u);
  EXPECT_EQ(reader.value(1), 5u);
  EXPECT_EQ(reader.slots()[0].type_id,
            static_cast<std::uint32_t>(salias::metrics::CounterType::ProducerPos));
  EXPECT_STREQ(reader.slots()[0].label, "producer_pos");
}

TEST(CountersTest, ForkedProcessWritesSharedMappingCounters) {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  ASSERT_GT(raw_page_size, 0);

  auto mapping_result = salias::platform::Mapping::create(
      salias::platform::MapOptions{.size = static_cast<std::size_t>(raw_page_size)});
  ASSERT_TRUE(mapping_result);
  auto mapping = std::move(mapping_result).value();

  auto region = std::span<std::byte>(mapping.as_ptr(), mapping.len());
  auto counters_result = salias::metrics::Counters::create_in(region, 2);
  ASSERT_TRUE(counters_result);
  auto counters = std::move(counters_result).value();
  counters.define(0, salias::metrics::CounterType::ProducerPos, 1, "producer_pos");

  const pid_t child = ::fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    auto child_counters = salias::metrics::Counters::view(region);
    if (!child_counters) {
      _exit(2);
    }
    child_counters.value().set_release(0, 4096);
    _exit(0);
  }

  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);

  auto reader_result = salias::metrics::CountersReader::view(region);
  ASSERT_TRUE(reader_result);
  EXPECT_EQ(reader_result.value().value(0), 4096u);
}

}  // namespace
