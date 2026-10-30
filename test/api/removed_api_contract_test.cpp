/**
 * @file test/api/removed_api_contract_test.cpp
 * @brief Verify the Phase 4 compatibility entry points stay removed.
 */
#include <salias/extras/extras.hpp>
#include <salias/salias.hpp>

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
  salias::extras::Ringbuffer<int, 2> queue;
  return queue.push(1) ? 1 : 0;
}
