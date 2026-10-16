module;
#include <salias/salias.hpp>

export module salias;

// Re-export the header API without changing its ABI or module ownership.
export namespace salias {
using ::salias::Channel;
using ::salias::Config;
using ::salias::Error;
using ::salias::FifoFanoutChannel;
using ::salias::FifoFanoutPublisher;
using ::salias::FifoFanoutSubscriber;
using ::salias::FifoMpscChannel;
using ::salias::FifoMpscPublisher;
using ::salias::FifoMpscSubscriber;
using ::salias::HugePage;
using ::salias::Message;
using ::salias::Mode;
using ::salias::OrderedFanoutChannel;
using ::salias::OrderedFanoutPublisher;
using ::salias::OrderedFanoutSubscriber;
using ::salias::OrderedMpscChannel;
using ::salias::OrderedMpscPublisher;
using ::salias::OrderedMpscSubscriber;
using ::salias::PublishClaim;
using ::salias::Publisher;
using ::salias::Result;
using ::salias::Ringbuffer;
using ::salias::Subscriber;
}  // namespace salias
