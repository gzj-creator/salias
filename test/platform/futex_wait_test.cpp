#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>

#if defined(__linux__)
#include <sys/syscall.h>
#endif

#include "core/wait/futex_wait.hpp"

namespace {

using salias::wait::AdaptiveFutex;
using salias::wait::FutexWaitResult;
using salias::wait::FutexWaitToken;

TEST(FutexWaitTest, DuplicateAddressesAreAcceptedAndDeadlineIsBounded) {
  alignas(std::uint32_t) std::uint32_t word = 0;
  const std::array tokens{FutexWaitToken{&word, 0}, FutexWaitToken{&word, 0}};
  const auto result = AdaptiveFutex::waitv(tokens, std::chrono::milliseconds{1});

  // Kernels without futex_waitv use the explicit caller fallback. Supported kernels return Woken
  // for both a timeout and a normal wake so the receive loop can rescan.
  EXPECT_NE(result, FutexWaitResult::PlatformFail);
}

TEST(FutexWaitTest, ChangedEpochReturnsToTheRescanPath) {
  alignas(std::uint32_t) std::uint32_t word = 1;
  const std::array tokens{FutexWaitToken{&word, 0}};
  const auto result = AdaptiveFutex::waitv(tokens, std::chrono::milliseconds{1});
  EXPECT_NE(result, FutexWaitResult::PlatformFail);
}

TEST(FutexWaitTest, MoreThanKernelVectorLimitUsesExplicitFallback) {
  alignas(std::uint32_t) std::array<std::uint32_t, 129> words{};
  std::array<FutexWaitToken, words.size()> tokens{};
  for (std::size_t index = 0; index < words.size(); ++index) {
    tokens[index] = FutexWaitToken{&words[index], 0};
  }
  EXPECT_EQ(AdaptiveFutex::waitv(tokens), FutexWaitResult::Unsupported);
}

#if defined(__linux__) && defined(SYS_futex_waitv)
TEST(FutexWaitTest, MisalignedEpochIsAPlatformError) {
  alignas(std::uint32_t) std::array<std::byte, sizeof(std::uint32_t) + 1> storage{};
  auto* misaligned = reinterpret_cast<std::uint32_t*>(storage.data() + 1);
  const std::array tokens{FutexWaitToken{misaligned, 0}};
  EXPECT_EQ(AdaptiveFutex::waitv(tokens), FutexWaitResult::PlatformFail);
}
#endif

#if defined(__linux__)
TEST(FutexWaitTest, DisabledWaitvUsesFallbackAndUnexpectedErrorsSurface) {
  EXPECT_EQ(salias::wait::detail::classify_futex_waitv_error(EPERM),
            FutexWaitResult::Unsupported);
  EXPECT_EQ(salias::wait::detail::classify_futex_waitv_error(ENOSYS),
            FutexWaitResult::Unsupported);
  EXPECT_EQ(salias::wait::detail::classify_futex_waitv_error(EINVAL),
            FutexWaitResult::PlatformFail);
}
#endif

}  // namespace
