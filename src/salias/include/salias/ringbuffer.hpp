/**
 * @file src/salias/include/salias/ringbuffer.hpp
 * @brief Compatibility forwarding header for the in-process Ringbuffer extra.
 */
#pragma once

#include "salias/extras/ringbuffer.hpp"

namespace salias {

template <class T, std::size_t Capacity, std::size_t HighWatermarkPercent = 90>
using Ringbuffer [[deprecated("use salias::extras::Ringbuffer")]] =
    extras::Ringbuffer<T, Capacity, HighWatermarkPercent>;

}  // namespace salias
