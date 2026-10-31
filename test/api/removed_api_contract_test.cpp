/**
 * @file test/api/removed_api_contract_test.cpp
 * @brief Verify that v6 extras and all former forwarding headers stay removed.
 */
#include <salias/salias.hpp>

#if __has_include(<salias/extras/extras.hpp>)
#error "extras umbrella header must remain removed"
#endif
#if __has_include(<salias/extras/latest_by_key_channel.hpp>)
#error "latest-by-key header must remain removed"
#endif
#if __has_include(<salias/extras/priority_channel.hpp>)
#error "priority header must remain removed"
#endif
#if __has_include(<salias/extras/ringbuffer.hpp>)
#error "ringbuffer header must remain removed"
#endif
#if __has_include(<salias/latest_by_key_channel.hpp>)
#error "legacy latest-by-key include must remain removed"
#endif
#if __has_include(<salias/priority_channel.hpp>)
#error "legacy priority include must remain removed"
#endif
#if __has_include(<salias/ringbuffer.hpp>)
#error "legacy ringbuffer include must remain removed"
#endif

int main() {
  salias::Config config;
  config.name = "removed-api-contract";
  return config.name.empty() ? 1 : 0;
}
