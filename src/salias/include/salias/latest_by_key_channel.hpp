/**
 * @file src/salias/include/salias/latest_by_key_channel.hpp
 * @brief Compatibility forwarding header for the latest-by-key extras.
 */
#pragma once

#include "salias/extras/latest_by_key_channel.hpp"

namespace salias {

template <class Key, class Value>
using LatestByKeyMessage [[deprecated("use salias::extras::LatestByKeyMessage")]] =
    extras::LatestByKeyMessage<Key, Value>;

template <class Key, class Value>
using LatestByKeyPublisher [[deprecated("use salias::extras::LatestByKeyPublisher")]] =
    extras::LatestByKeyPublisher<Key, Value>;

template <class Key, class Value, std::size_t Capacity>
using LatestByKeySubscriber [[deprecated("use salias::extras::LatestByKeySubscriber")]] =
    extras::LatestByKeySubscriber<Key, Value, Capacity>;

template <class Key, class Value, std::size_t Capacity, std::uint64_t SchemaVersion = 0>
using LatestByKeyChannel [[deprecated("use salias::extras::LatestByKeyChannel")]] =
    extras::LatestByKeyChannel<Key, Value, Capacity, SchemaVersion>;

}  // namespace salias
