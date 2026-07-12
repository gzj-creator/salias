#include <salias/channel.hpp>

#include <cstddef>
#include <string>

#include <unistd.h>

int main() {
  const long system_page_size = ::sysconf(_SC_PAGESIZE);
  if (system_page_size <= 1) {
    return 1;
  }

  salias::Config small_ring;
  small_ring.name = "capacity-contract-small-" + std::to_string(::getpid());
  small_ring.capacity = static_cast<std::size_t>(system_page_size) / 2;
  auto small_created = salias::FifoMpscChannel::create(small_ring);
  if (small_created || small_created.error() != salias::Error::BadConfig) {
    return 2;
  }

  salias::Config huge_ring;
  huge_ring.name = "capacity-contract-huge-" + std::to_string(::getpid());
  huge_ring.capacity = 1u << 20;
  huge_ring.huge = salias::HugePage::Size2MB;
  auto huge_created = salias::FifoMpscChannel::create(huge_ring);
  if (huge_created || huge_created.error() != salias::Error::BadConfig) {
    return 3;
  }
}
