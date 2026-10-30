/**
 * @file test/api/extras_compat_test.cpp
 * @brief Lock the deprecated top-level extras aliases to their canonical names.
 */
#include <salias/extras/extras.hpp>
#include <salias/latest_by_key_channel.hpp>
#include <salias/priority_channel.hpp>
#include <salias/ringbuffer.hpp>
#include <type_traits>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

static_assert(std::is_same_v<salias::LatestByKeyMessage<std::uint64_t, std::uint64_t>,
                             salias::extras::LatestByKeyMessage<std::uint64_t, std::uint64_t>>);
static_assert(std::is_same_v<salias::LatestByKeyPublisher<std::uint64_t, std::uint64_t>,
                             salias::extras::LatestByKeyPublisher<std::uint64_t, std::uint64_t>>);
static_assert(std::is_same_v<salias::LatestByKeySubscriber<std::uint64_t, std::uint64_t, 4>,
                             salias::extras::LatestByKeySubscriber<std::uint64_t, std::uint64_t, 4>>);
static_assert(std::is_same_v<salias::LatestByKeyChannel<std::uint64_t, std::uint64_t, 4>,
                             salias::extras::LatestByKeyChannel<std::uint64_t, std::uint64_t, 4>>);
static_assert(std::is_same_v<salias::PriorityWaitPolicy, salias::extras::PriorityWaitPolicy>);
static_assert(std::is_same_v<salias::PriorityMessage, salias::extras::PriorityMessage>);
static_assert(std::is_same_v<salias::PriorityPublisher<salias::Mode::FifoMpsc,
                                                       salias::Mode::FifoMpsc>,
                             salias::extras::PriorityPublisher<salias::Mode::FifoMpsc,
                                                               salias::Mode::FifoMpsc>>);
static_assert(std::is_same_v<salias::PrioritySubscriber<salias::Mode::FifoMpsc,
                                                        salias::Mode::FifoMpsc>,
                             salias::extras::PrioritySubscriber<salias::Mode::FifoMpsc,
                                                                salias::Mode::FifoMpsc>>);
static_assert(std::is_same_v<salias::Ringbuffer<int, 8>, salias::extras::Ringbuffer<int, 8>>);

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

int main() {
  salias::extras::Ringbuffer<int, 2> queue;
  return queue.push(1) ? 1 : 0;
}
