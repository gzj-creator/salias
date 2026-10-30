module;
#include <salias/salias.hpp>
#include <salias/extras/extras.hpp>

export module salias.extras;

// Re-export optional extras without adding them to import salias;.
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
export namespace salias::extras {
using ::salias::extras::LatestByKeyChannel;
using ::salias::extras::LatestByKeyMessage;
using ::salias::extras::LatestByKeyPublisher;
using ::salias::extras::LatestByKeySubscriber;
using ::salias::extras::PriorityMessage;
using ::salias::extras::PriorityPublisher;
using ::salias::extras::PrioritySubscriber;
using ::salias::extras::PriorityWaitPolicy;
using ::salias::extras::Ringbuffer;
}  // namespace salias::extras
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
