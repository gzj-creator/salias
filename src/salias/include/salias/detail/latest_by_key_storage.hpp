/**
 * @file src/salias/include/salias/detail/latest_by_key_storage.hpp
 * @brief Internal type-erased shared-memory storage for LatestByKeyChannel and its endpoints.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "salias/error.hpp"

namespace salias::detail {

struct LatestByKeyStorage;

struct LatestByKeySlotView {
  std::uint64_t* commit = nullptr;
  std::uint64_t* hash = nullptr;
  std::byte* key = nullptr;
  std::byte* values[2]{};
};

struct LatestByKeyEventView {
  std::uint64_t* sequence = nullptr;
  std::uint64_t* slot_index = nullptr;
};

struct LatestByKeyTableView {
  std::byte* slots = nullptr;
  std::byte* events = nullptr;
  std::uint64_t* event_sequence = nullptr;
  std::size_t slot_count = 0;
  std::size_t event_count = 0;
  std::size_t stride = 0;
  std::size_t key_offset = 0;
  std::size_t value0_offset = 0;
  std::size_t value1_offset = 0;

  LatestByKeySlotView slot(std::size_t index) const noexcept {
    std::byte* base = slots + index * stride;
    return {
        .commit = reinterpret_cast<std::uint64_t*>(base),
        .hash = reinterpret_cast<std::uint64_t*>(base + sizeof(std::uint64_t)),
        .key = base + key_offset,
        .values = {base + value0_offset, base + value1_offset},
    };
  }

  LatestByKeyEventView event(std::size_t index) const noexcept {
    auto* words = reinterpret_cast<std::uint64_t*>(events + index * 2 * sizeof(std::uint64_t));
    return {.sequence = &words[0], .slot_index = &words[1]};
  }
};

using LatestByKeyTableCallback = Error (*)(LatestByKeyTableView, void*) noexcept;

Result<std::shared_ptr<LatestByKeyStorage>> create_latest_by_key_storage(
    std::string_view name, std::size_t slot_count, std::size_t key_size, std::size_t value_size,
    std::uint64_t key_schema, std::uint64_t value_schema);

Result<std::shared_ptr<LatestByKeyStorage>> connect_latest_by_key_storage(
    std::string_view name, std::size_t slot_count, std::size_t key_size, std::size_t value_size,
    std::uint64_t key_schema, std::uint64_t value_schema);

Error with_latest_by_key_table(const std::shared_ptr<LatestByKeyStorage>& storage,
                               LatestByKeyTableCallback callback, void* context,
                               bool wait_for_lock = false) noexcept;

}  // namespace salias::detail
