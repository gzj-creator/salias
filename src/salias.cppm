module;
#include <salias/salias.hpp>

export module salias;

// Re-export the header API without changing its ABI or module ownership.
export namespace salias {
using ::salias::Channel;
using ::salias::Config;
using ::salias::Error;
using ::salias::FifoMpmcChannel;
using ::salias::FifoMpmcPublisher;
using ::salias::FifoMpmcSubscriber;
using ::salias::FifoMpscChannel;
using ::salias::FifoMpscPublisher;
using ::salias::FifoMpscSubscriber;
using ::salias::HugePage;
using ::salias::LatestByKeyChannel;
using ::salias::LatestByKeyMessage;
using ::salias::LatestByKeyPublisher;
using ::salias::LatestByKeySubscriber;
using ::salias::LatestMpmcChannel;
using ::salias::LatestMpmcPublisher;
using ::salias::LatestMpmcSubscriber;
using ::salias::Message;
using ::salias::Mode;
using ::salias::OrderedMpmcChannel;
using ::salias::OrderedMpmcPublisher;
using ::salias::OrderedMpmcSubscriber;
using ::salias::OrderedMpscChannel;
using ::salias::OrderedMpscPublisher;
using ::salias::OrderedMpscSubscriber;
using ::salias::PriorityMessage;
using ::salias::PriorityPublisher;
using ::salias::PrioritySubscriber;
using ::salias::PublishClaim;
using ::salias::Publisher;
using ::salias::Result;
using ::salias::Ringbuffer;
using ::salias::Subscriber;
using ::salias::SubscriptionPolicy;
}  // namespace salias
