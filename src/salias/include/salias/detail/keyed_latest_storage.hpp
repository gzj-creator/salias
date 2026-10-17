/**
 * @file src/salias/include/salias/detail/keyed_latest_storage.hpp
 * @brief Internal type-erased shared-memory storage for KeyedLatestChannel.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "salias/error.hpp"

namespace salias::detail {

struct KeyedLatestStorage;

struct KeyedLatestSlotView {
  std::uint64_t* commit = nullptr;
  std::uint64_t* hash = nullptr;
  std::byte* key = nullptr;
  std::byte* values[2]{};
};

struct KeyedLatestEventView {
  std::uint64_t* sequence = nullptr;
  std::uint64_t* slot_index = nullptr;
};

struct KeyedLatestTableView {
  std::byte* slots = nullptr;
  std::byte* events = nullptr;
  std::uint64_t* event_sequence = nullptr;
  std::size_t slot_count = 0;
  std::size_t event_count = 0;
  std::size_t stride = 0;
  std::size_t key_offset = 0;
  std::size_t value0_offset = 0;
  std::size_t value1_offset = 0;

  KeyedLatestSlotView slot(std::size_t index) const noexcept {
    std::byte* base = slots + index * stride;
    return {
        .commit = reinterpret_cast<std::uint64_t*>(base),
        .hash = reinterpret_cast<std::uint64_t*>(base + sizeof(std::uint64_t)),
        .key = base + key_offset,
        .values = {base + value0_offset, base + value1_offset},
    };
  }

  KeyedLatestEventView event(std::size_t index) const noexcept {
    auto* words = reinterpret_cast<std::uint64_t*>(events + index * 2 * sizeof(std::uint64_t));
    return {.sequence = &words[0], .slot_index = &words[1]};
  }
};

using KeyedLatestTableCallback = Error (*)(KeyedLatestTableView, void*) noexcept;

Result<std::shared_ptr<KeyedLatestStorage>> create_keyed_latest_storage(
    std::string_view name, std::size_t slot_count, std::size_t key_size,
    std::size_t value_size, std::uint64_t key_schema, std::uint64_t value_schema);

Result<std::shared_ptr<KeyedLatestStorage>> connect_keyed_latest_storage(
    std::string_view name, std::size_t slot_count, std::size_t key_size,
    std::size_t value_size, std::uint64_t key_schema, std::uint64_t value_schema);

Error with_keyed_latest_table(const std::shared_ptr<KeyedLatestStorage>& storage,
                              KeyedLatestTableCallback callback, void* context,
                              bool wait_for_lock = false) noexcept;

}  // namespace salias::detail
