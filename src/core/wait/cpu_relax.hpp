#pragma once

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace salias::wait {

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  _mm_pause();
#else
  __asm__ __volatile__("yield" ::: "memory");
#endif
}

}  // namespace salias::wait
