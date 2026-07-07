#pragma once

#include <cstddef>
#include <expected>

#include "core/platform/error.hpp"
#include "core/platform/map_options.hpp"

namespace salias::platform {

class Mapping {
 public:
  using CreateResult = std::expected<Mapping, PlatformError>;

  // 创建 Linux magic-ring 双映射，两段相邻虚拟地址共享同一 memfd。
  // options.size 必须非零、为 2 的幂且页对齐；系统调用失败时返回 PlatformError。
  static CreateResult create(const MapOptions& options) noexcept;

  // 基于已打开的 MAP_SHARED fd 创建双映射；Mapping 持有 dup 后的 fd。
  // 原始 fd 仍归调用者所有，且文件长度必须至少为 options.size。
  static CreateResult map_shared_fd(int fd, const MapOptions& options) noexcept;

  // 创建空映射句柄。
  Mapping() noexcept = default;
  // 从另一个句柄转移映射和 fd 所有权。
  Mapping(Mapping&& other) noexcept;
  // 释放当前映射后转移另一个句柄的所有权。
  Mapping& operator=(Mapping&& other) noexcept;
  // 禁止拷贝；Mapping 独占虚拟映射和 fd。
  Mapping(const Mapping&) = delete;
  // 禁止拷贝赋值；Mapping 独占虚拟映射和 fd。
  Mapping& operator=(const Mapping&) = delete;
  // 释放双映射区并关闭持有的 fd。
  ~Mapping() noexcept;

  // 返回第一段虚拟地址指针；可用映射范围为 [as_ptr(), as_ptr()+2*len())。
  // 指针仅在当前对象仍持有映射且未被移动后有效。
  std::byte* as_ptr() const noexcept { return base_; }
  // 返回单段逻辑容量；第二段从 as_ptr()+len() 开始。
  std::size_t len() const noexcept { return len_; }

 private:
  // 将已持有的 fd 映射两次到一个连续虚拟地址区间。
  static CreateResult map_owned_fd(int fd, std::size_t size, bool self_check) noexcept;

  // 保存映射成功后的起始地址、逻辑长度和持有 fd。
  Mapping(std::byte* base, std::size_t len, int fd) noexcept;
  // 释放当前映射并将对象重置为空。
  void reset() noexcept;

  std::byte* base_ = nullptr;
  std::size_t len_ = 0;
  int fd_ = -1;
};

}  // namespace salias::platform
