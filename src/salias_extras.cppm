module;
#include <salias/salias.hpp>
#include <salias/extras/extras.hpp>

export module salias.extras;

// Re-export optional extras without adding them to import salias;.
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
