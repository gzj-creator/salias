#pragma once

#include <cstddef>

#include "core/platform/error.hpp"
#include "core/platform/map_options.hpp"
#include "core/platform/result.hpp"

namespace salias::platform {

class Mapping {
 public:
  using CreateResult = Result<Mapping, PlatformError>;

  // Creates a Linux magic-ring mapping with two adjacent virtual ranges backed by the same memfd.
  // options.size must be non-zero, a power of two, page-aligned, and must not overflow 2*size.
  // Returns explicit PlatformError values; construction never throws for syscall failures.
  static CreateResult create(const MapOptions& options) noexcept;

  Mapping() noexcept = default;
  Mapping(Mapping&& other) noexcept;
  Mapping& operator=(Mapping&& other) noexcept;
  Mapping(const Mapping&) = delete;
  Mapping& operator=(const Mapping&) = delete;
  ~Mapping() noexcept;

  // Pointer to the first virtual range. The usable mapped address space is [as_ptr(), as_ptr() +
  // 2*len()), but len() returns the logical single-ring capacity. The pointer remains valid only
  // while this Mapping object owns the mapping and has not been moved from.
  std::byte* as_ptr() const noexcept { return base_; }
  // Logical single-segment capacity. The second segment starts at as_ptr() + len().
  std::size_t len() const noexcept { return len_; }

 private:
  Mapping(std::byte* base, std::size_t len, int fd) noexcept;
  void reset() noexcept;

  std::byte* base_ = nullptr;
  std::size_t len_ = 0;
  int fd_ = -1;
};

}  // namespace salias::platform
