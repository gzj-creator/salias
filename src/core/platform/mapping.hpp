/**
 * @file src/core/platform/mapping.hpp
 * @brief Linux/macOS magic-ring 双映射（mmap）后端存储的 RAII 句柄。
 * @details 本文件位于 L0 平台层，为 L1 ring 提供底层共享内存后端。
 * 核心技巧：将同一个 memfd 以 offset 0 映射到两段相邻的虚拟地址区间，
 * 构造出"首尾相接"的 2*len 连续虚拟空间，使 ring 的 producer/consumer
 * 无需对 sequence 做 wraparound（低位环绕）判断即可线性写入/读取。
 * Mapping 通过 RAII 独占管理虚拟映射与文件描述符：移动语义转移所有权，
 * 禁止拷贝；析构时统一 munmap 双映射并 close fd。所有创建路径返回
 * std::expected<Mapping, PlatformError>，绝不抛异常。
 */
#pragma once

#include <cstddef>
#include <expected>

#include "core/platform/error.hpp"
#include "core/platform/map_options.hpp"

namespace salias::platform {

/// L0 平台层命名空间，封装 mmap/memfd_create/huge page 等系统资源管理。

/**
 * @brief Linux/macOS magic-ring 双映射后端的 RAII 句柄。
 * @details Linux 通过 memfd_create、macOS 通过 unlink 后的临时文件创建匿名后端，
 * 并以 MAP_FIXED 将其
 * 同一 offset 映射到两段相邻虚拟地址，使上层 ring 获得逻辑上的环形空间。
 *
 * 所有权：独占（move-only）。构造由静态工厂 create()/map_shared_fd() 完成，
 * 析构自动释放双映射并关闭 fd；移动构造/赋值转移所有权并清空来源对象。
 *
 * 内存布局：[base_, base_+len_) 与 [base_+len_, base_+2*len_) 共享同一物理页，
 * 物理后端实际容量仅为 len_ 字节（由 ftruncate 设定）。
 *
 * 线程安全：单个 Mapping 实例本身不可并发访问；移动构造/赋值非线程安全。
 * 不同 Mapping 实例各自独立。映射后的共享内存可由多进程/多线程并发访问，
 * 同步由上层 ring 的 atomic 操作保证。
 */
class Mapping {
 public:
  /// 工厂方法返回类型，成功携带 Mapping，失败携带 PlatformError。
  using CreateResult = std::expected<Mapping, PlatformError>;

  /**
   * @brief 创建新的匿名文件后端并构造双映射。
   * @param options 映射选项（容量、大页类型、NUMA 节点）。
   * @retval Mapping 创建成功的句柄。
   * @retval PlatformError 系统调用失败时的错误码。
   * @note options.size 须非零、为 2 的幂且页对齐；显式大页请求还须按大页大小对齐。
   *       大页资源不足时返回 HugePageUnavailable。该函数 noexcept，绝不抛异常。
   */
  // 创建 POSIX magic-ring 双映射，两段相邻虚拟地址共享同一文件后端。
  // options.size 必须非零、为 2 的幂且页对齐；显式大页请求还必须按大页大小对齐。
  // 系统调用失败时返回 PlatformError，且大页资源不足返回 HugePageUnavailable。
  static CreateResult create(const MapOptions& options) noexcept;

  /**
   * @brief 基于已打开的 MAP_SHARED fd 创建双映射。
   * @param fd 调用者持有的已打开共享文件描述符（不被本函数关闭）。
   * @param options 映射选项；size 须不超过 fd backing store 实际长度。
   * @retval Mapping 创建成功的句柄，内部持有 dup 后的新 fd。
   * @retval PlatformError 校验或映射失败时的错误码。
   * @note 原始 fd 仍归调用者所有；Mapping 通过 dup 独占自身持有的副本。
   *       若 options.huge 非 None，调用者必须保证 fd 已经由匹配的大页后端创建。
   */
  // 基于已打开的 MAP_SHARED fd 创建双映射；Mapping 持有 dup 后的 fd。
  // 原始 fd 仍归调用者所有，且文件长度必须至少为 options.size。
  // 若 options.huge 非 None，调用者必须保证 fd 已经由匹配的大页后端创建。
  static CreateResult map_shared_fd(int fd, const MapOptions& options) noexcept;

  /// 创建空映射句柄（base_ 为 nullptr，不持有任何资源）。
  // 创建空映射句柄。
  Mapping() noexcept = default;
  /**
   * @brief 从另一个句柄转移映射和 fd 所有权。
   * @param other 来源句柄，移动后变为空。
   * @note 移动后 other 不再持有任何映射或 fd，非线程安全。
   */
  // 从另一个句柄转移映射和 fd 所有权。
  Mapping(Mapping&& other) noexcept;
  /**
   * @brief 释放当前映射后转移另一个句柄的所有权。
   * @param other 来源句柄，移动后变为空。
   * @return *this。
   * @note 自赋值安全（做自检）；先 reset() 当前资源再转移，非线程安全。
   */
  // 释放当前映射后转移另一个句柄的所有权。
  Mapping& operator=(Mapping&& other) noexcept;
  /// 禁止拷贝构造；Mapping 独占虚拟映射和 fd。
  // 禁止拷贝；Mapping 独占虚拟映射和 fd。
  Mapping(const Mapping&) = delete;
  /// 禁止拷贝赋值；Mapping 独占虚拟映射和 fd。
  // 禁止拷贝赋值；Mapping 独占虚拟映射和 fd。
  Mapping& operator=(const Mapping&) = delete;
  /**
   * @brief 释放双映射区并关闭持有的 fd。
   * @note 调用 reset() 完成资源释放；对空句柄是空操作。
   */
  // 释放双映射区并关闭持有的 fd。
  ~Mapping() noexcept;

  /**
   * @brief 返回第一段虚拟地址的起始指针。
   * @return 指向 [base_, base_+2*len_) 连续可用范围的起始字节指针。
   * @note 可用映射范围为 [as_ptr(), as_ptr()+2*len())。指针仅在当前对象
   *       仍持有映射且未被移动/析构后有效。
   */
  // 返回第一段虚拟地址指针；可用映射范围为 [as_ptr(), as_ptr()+2*len())。
  // 指针仅在当前对象仍持有映射且未被移动后有效。
  std::byte* as_ptr() const noexcept { return base_; }
  /**
   * @brief 返回单段逻辑容量。
   * @return 逻辑容量字节数；第二段（镜像）从 as_ptr()+len() 开始。
   */
  // 返回单段逻辑容量；第二段从 as_ptr()+len() 开始。
  std::size_t len() const noexcept { return len_; }

 private:
  /**
   * @brief 将已持有的 fd 映射两次到一个连续虚拟地址区间。
   * @param fd 已持有的文件描述符，映射失败时由本函数负责关闭。
   * @param size 单段逻辑容量（字节）。
   * @param self_check 是否执行双别名物理页共享的运行时自检。
   * @param alignment 大页对齐粒度（0 或大页大小）。
   * @param huge_requested 是否请求了大页（影响错误码映射）。
   * @retval Mapping 映射成功。
   * @retval PlatformError 预留或 MAP_FIXED 失败。
   * @note 内部辅助函数，供 create()/map_shared_fd() 复用双映射逻辑。
   */
  // 将已持有的 fd 映射两次到一个连续虚拟地址区间。
  static CreateResult map_owned_fd(int fd, std::size_t size, bool self_check, std::size_t alignment,
                                   bool huge_requested) noexcept;

  /**
   * @brief 保存映射成功后的起始地址、逻辑长度和持有 fd。
   * @param base 双映射起始虚拟地址。
   * @param len 单段逻辑容量。
   * @param fd 持有的文件描述符（由本对象负责关闭）。
   */
  // 保存映射成功后的起始地址、逻辑长度和持有 fd。
  Mapping(std::byte* base, std::size_t len, int fd) noexcept;
  /// 释放当前映射并将对象重置为空（base_ 置空、fd_ 置 -1）。
  // 释放当前映射并将对象重置为空。
  void reset() noexcept;

  std::byte* base_ = nullptr;  ///< 双映射起始虚拟地址；nullptr 表示空句柄。
  std::size_t len_ = 0;        ///< 单段逻辑容量（字节）；双映射实际占用 2*len_ 虚拟地址。
  int fd_ = -1;                ///< 持有的匿名文件描述符；-1 表示未持有。
};

}  // namespace salias::platform
