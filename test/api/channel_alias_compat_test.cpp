/**
 * @file test/api/channel_alias_compat_test.cpp
 * @brief 验证四个旧 Channel 兼容别名在弃用窗口内仍保持同型。
 */
#include <salias/salias.hpp>
#include <type_traits>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

static_assert(std::is_same_v<salias::FifoMpscChannel, salias::Channel<salias::Mode::FifoMpsc>>);
static_assert(std::is_same_v<salias::FifoMpmcChannel, salias::Channel<salias::Mode::FifoMpmc>>);
static_assert(
    std::is_same_v<salias::OrderedMpscChannel, salias::Channel<salias::Mode::OrderedMpsc>>);
static_assert(
    std::is_same_v<salias::OrderedMpmcChannel, salias::Channel<salias::Mode::OrderedMpmc>>);

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

int main() { return 0; }
