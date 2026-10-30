/**
 * @file src/salias/include/salias/priority_channel.hpp
 * @brief Compatibility forwarding header for priority-routing extras.
 */
#pragma once

#include "salias/extras/priority_channel.hpp"

namespace salias {

using PriorityWaitPolicy [[deprecated("use salias::extras::PriorityWaitPolicy")]] =
    extras::PriorityWaitPolicy;
using PriorityMessage [[deprecated("use salias::extras::PriorityMessage")]] =
    extras::PriorityMessage;

template <Mode... Modes>
using PriorityPublisher [[deprecated("use salias::extras::PriorityPublisher")]] =
    extras::PriorityPublisher<Modes...>;

template <Mode... Modes>
using PrioritySubscriber [[deprecated("use salias::extras::PrioritySubscriber")]] =
    extras::PrioritySubscriber<Modes...>;

}  // namespace salias
