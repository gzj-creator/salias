/**
 * @file src/core/platform/asymmetric_fence.hpp
 * @brief 非对称屏障：把 claim 热路径上的完整屏障搬到订阅者注册/注销的冷路径。
 * @details 位于 L0 平台层。claim 与注册的 Dekker 握手由两侧共同保证：
 *          - 发布端（light side）：置位 claim_active 只需一次 relaxed store 加编译器屏障；
 *          - 注册端（heavy side）：写完 registration_gate 后调用 membarrier，
 *            让所有已注册进程的线程在返回前执行一次完整屏障。
 *          其正确性依赖“写 gate 的线程先调用 membarrier，发布进程已注册为
 *          MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED 接收方”这一对前提：若发布线程的
 *          gate 读发生在 IPI 之前，则它先前的 claim_active store 必然在 membarrier
 *          返回前对该注册端可见；若 IPI 先到，则发布线程随后的 gate 读必然看到已置位的
 *          gate 并放弃本次 claim。因此“claim 读到 gate 置位”与“注册端看到 claim_active
 *          置位”不可能同时为假。
 *          非 Linux 平台或运维开关关闭时，通道协议退回 seq_cst，热路径行为与 v14 完全一致。
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <unistd.h>

#if defined(__linux__)
#include <linux/membarrier.h>
#include <sys/syscall.h>
#endif

/// L0 平台层命名空间：mmap/共享内存/时钟/屏障等系统原语封装。
namespace salias::platform::fence {

/**
 * @brief 通道级 claim 屏障协议，写入命名控制段后不再改变。
 * @details 枚举值参与跨进程协议，必须保持稳定。取值 0 与 v14 语义一致，便于识别旧布局。
 */
enum class ClaimFenceProtocol : std::uint8_t {
  kSeqCst = 0,      ///< 每次 claim 用两条 seq_cst store（x86 上即两次完整屏障）。
  kMembarrier = 1,  ///< claim 只保留编译器屏障，由注册端 membarrier 提供完整屏障。
};

/// @brief 单个发布进程的 membarrier 注册结果。
enum class RegistrationStatus {
  kRegistered,  ///< 本进程已注册为 GLOBAL_EXPEDITED 接收方，可使用 light path。
  kUnsupported,  ///< 平台或内核不支持该命令。
  kFailed,       ///< 注册系统调用失败。
};

/**
 * @brief 读取 SALIAS_DISABLE_ASYMMETRIC_FENCE 运维开关。
 * @return true 表示允许使用非对称屏障；变量缺失或取值 "0" 时视为允许。
 * @note 只影响建表端选择通道协议，不影响已经建成通道的 heavy side 行为；
 *       运维方关闭开关后，老通道仍按各自协议执行，避免出现只拆一侧屏障的半成品状态。
 */
inline bool asymmetric_fence_allowed_by_environment() noexcept {
  const char* value = std::getenv("SALIAS_DISABLE_ASYMMETRIC_FENCE");
  if (value == nullptr || value[0] == '\0' || std::strcmp(value, "0") == 0) {
    return true;
  }
  return false;
}

namespace detail {

/// 进程级注册状态：0 未尝试，1 注册中，2 注册成功，-1 失败。
inline std::atomic<int> g_registration_state{0};
/// 正在执行注册的进程 ID，用于识别 fork 时继承下来的“注册中”状态。
inline std::atomic<int> g_registration_owner_pid{0};
/**
 * @brief 故障注入开关（测试专用）。
 * @note 变量本身在所有构建中都存在，但只有 SALIAS_TEST_HOOKS 构建能通过下面的
 *       setter 写入；生产构建中它恒为 false，仅表现为冷路径上的一次 relaxed load。
 */
inline std::atomic<bool> g_force_registration_failure{false};
inline std::atomic<bool> g_force_heavy_fence_failure{false};

#if defined(__linux__)
/// @brief 直接走 syscall，避免依赖 glibc 是否导出 membarrier() 包装函数。
inline long membarrier_syscall(int command) noexcept {
  return ::syscall(SYS_membarrier, command, 0, 0);
}
#endif

/**
 * @brief 查询内核支持的 membarrier 命令位图（结果缓存一次）。
 * @return 支持的命令位掩码；不支持时返回 -1。
 */
inline std::int32_t query_supported_commands() noexcept {
#if defined(__linux__)
  static std::atomic<std::int32_t> cached{-2};  // 常量初始化，无动态初始化保护变量。
  std::int32_t value = cached.load(std::memory_order_acquire);
  if (value == -2) {
    const long result = membarrier_syscall(MEMBARRIER_CMD_QUERY);
    value = result < 0 ? -1 : static_cast<std::int32_t>(result);
    cached.store(value, std::memory_order_release);
  }
  return value;
#else
  return -1;
#endif
}

}  // namespace detail

/**
 * @brief 查询本机是否支持 GLOBAL_EXPEDITED 及其注册命令。
 * @return true 表示可使用非对称屏障协议。
 */
inline bool global_expedited_supported() noexcept {
#if defined(__linux__)
  const std::int32_t supported = detail::query_supported_commands();
  if (supported < 0) {
    return false;
  }
  const auto mask = static_cast<std::uint32_t>(supported);
  return (mask & MEMBARRIER_CMD_GLOBAL_EXPEDITED) != 0 &&
         (mask & MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED) != 0;
#else
  return false;
#endif
}

/**
 * @brief 建表端选择本通道的 claim 屏障协议。
 * @return 支持且未被运维开关关闭时返回 kMembarrier，否则返回 kSeqCst。
 * @note 该选择与“当前进程能否注册”无关：通道只是允许使用非对称屏障，
 *       各发布进程在构造 Publisher 时独立决定是否真正采用 light path。
 */
inline ClaimFenceProtocol select_channel_protocol() noexcept {
  if (!asymmetric_fence_allowed_by_environment()) {
    return ClaimFenceProtocol::kSeqCst;
  }
  return global_expedited_supported() ? ClaimFenceProtocol::kMembarrier
                                      : ClaimFenceProtocol::kSeqCst;
}

/**
 * @brief 把当前进程注册为 GLOBAL_EXPEDITED 接收方（每进程只做一次）。
 * @return 注册状态；已注册时直接返回 kRegistered。
 * @note 用原子状态而非 std::call_once，避免“另一线程正在注册时 fork”把子进程永久挂在
 *       once 初始化上。父子进程共享同一 mm 时注册状态随之继承；exec 换 mm 后需重新注册，
 *       本函数的状态机在子进程里会因 owner pid 变化而重新执行注册。
 */
inline RegistrationStatus register_publisher_process() noexcept {
  if (detail::g_force_registration_failure.load(std::memory_order_relaxed)) {
    return RegistrationStatus::kFailed;
  }
  auto& state = detail::g_registration_state;
  auto& owner = detail::g_registration_owner_pid;
  const int self_pid = static_cast<int>(::getpid());
  for (;;) {
    const int observed = state.load(std::memory_order_acquire);
    if (observed == 2) {
      return RegistrationStatus::kRegistered;
    }
    if (observed == -1) {
      return RegistrationStatus::kFailed;
    }
    if (observed == 1 && owner.load(std::memory_order_relaxed) == self_pid) {
      // 同进程内另一个线程正在注册，等它完成（注册只涉及两次系统调用）。
      std::this_thread::yield();
      continue;
    }
    if (observed == 1) {
      // 父进程在注册过程中 fork 出了本进程：继承来的“注册中”不属于本进程，回收重试。
      int expected = 1;
      static_cast<void>(state.compare_exchange_strong(expected, 0, std::memory_order_acq_rel,
                                                      std::memory_order_acquire));
      continue;
    }
    owner.store(self_pid, std::memory_order_relaxed);
    int expected = 0;
    if (!state.compare_exchange_strong(expected, 1, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
      continue;
    }
#if defined(__linux__)
    if (global_expedited_supported() &&
        detail::membarrier_syscall(MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED) == 0) {
      state.store(2, std::memory_order_release);
      return RegistrationStatus::kRegistered;
    }
    state.store(-1, std::memory_order_release);
    return global_expedited_supported() ? RegistrationStatus::kFailed
                                        : RegistrationStatus::kUnsupported;
#else
    state.store(-1, std::memory_order_release);
    return RegistrationStatus::kUnsupported;
#endif
  }
}

/// @brief light side 屏障：只阻止编译器把 claim_active 的置位挪到 gate 读之后。
/// @note 生成零指令（x86/ARM 均为空），不产生任何硬件屏障。
inline void light_fence() noexcept { std::atomic_signal_fence(std::memory_order_seq_cst); }

/**
 * @brief heavy side 屏障：让所有已注册进程的线程在返回前执行一次完整屏障。
 * @return true 表示已建立跨进程完整屏障；false 表示无法建立，调用方必须 fail closed。
 * @details 优先 GLOBAL_EXPEDITED（需要发布端已注册，调用端自身不必注册），
 *          失败退回 GLOBAL（全局 IPI，较慢但不需要注册）。
 */
inline bool heavy_fence() noexcept {
  if (detail::g_force_heavy_fence_failure.load(std::memory_order_relaxed)) {
    return false;
  }
#if defined(__linux__)
  if (detail::membarrier_syscall(MEMBARRIER_CMD_GLOBAL_EXPEDITED) == 0) {
    return true;
  }
  if (detail::membarrier_syscall(MEMBARRIER_CMD_GLOBAL) == 0) {
    return true;
  }
  return false;
#else
  // 非 Linux 平台不会选择 kMembarrier 协议，这里的本地完整屏障仅作防御性兜底。
  std::atomic_thread_fence(std::memory_order_seq_cst);
  return true;
#endif
}

/// @brief 本进程是否已成功注册，可对 kMembarrier 通道使用 light path。
inline bool publisher_light_path_ready() noexcept {
  return detail::g_registration_state.load(std::memory_order_acquire) == 2;
}

/// @brief 返回本进程的 membarrier 注册位图（无命令时返回 0），用于测试与诊断。
/// @note MEMBARRIER_CMD_GET_REGISTRATIONS 在 linux/membarrier.h 中是枚举常量而非宏，
///       不能用 #if defined(...) 探测，因此这里直接写等价字面量（1 << 9）。
inline std::uint32_t registered_commands() noexcept {
#if defined(__linux__)
  constexpr int kCommandGetRegistrations = 1 << 9;  // MEMBARRIER_CMD_GET_REGISTRATIONS
  const long result = detail::membarrier_syscall(kCommandGetRegistrations);
  return result < 0 ? 0u : static_cast<std::uint32_t>(result);
#else
  return 0u;
#endif
}

#if defined(SALIAS_TEST_HOOKS)
/// @brief 测试钩子：强制发布端注册失败，验证退回 seq_cst 的路径。
inline void set_registration_failure_for_testing(bool enabled) noexcept {
  detail::g_force_registration_failure.store(enabled, std::memory_order_relaxed);
}

/// @brief 测试钩子：强制 heavy fence 失败，验证注册端 fail closed 行为。
inline void set_heavy_fence_failure_for_testing(bool enabled) noexcept {
  detail::g_force_heavy_fence_failure.store(enabled, std::memory_order_relaxed);
}

/// @brief 测试钩子：清除全部故障注入开关。
inline void clear_failure_injection_for_testing() noexcept {
  detail::g_force_registration_failure.store(false, std::memory_order_relaxed);
  detail::g_force_heavy_fence_failure.store(false, std::memory_order_relaxed);
}

/// @brief 测试钩子：把进程级注册状态重置为“未尝试”。
/// @note 只重置用户态状态机，不影响内核里本 mm 已有的注册位；重置后下一次
///       register_publisher_process() 会重新走一遍注册系统调用（幂等成功）。
inline void reset_registration_state_for_testing() noexcept {
  detail::g_registration_state.store(0, std::memory_order_release);
}
#endif

}  // namespace salias::platform::fence
