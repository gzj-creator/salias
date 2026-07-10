# CAS Backoff Plan (Phase 3.5) — Fix Unbatched Multi-Producer Negative Scaling

Companion to `2026-07-08-throughput-optimization-design.md` and
`-throughput-optimization.md`. This is the cheap experiment that must run before
committing to the expensive Phase 4 (independent/rotating rings).

## Motivation (evidence from the A/B round)

The batched path already beats Aeron in every scenario (MPSC 1.14–1.32x, MPMC
1.13–1.44x). The remaining defect is **unbatched multi-producer negative
scaling**, proven by the 10M-msg Tencent run:

| Metric | Value | Meaning |
| ------ | ----- | ------- |
| 1P/1C normal | 26.610 M/s | single-producer, zero contention |
| 4P/1C batch=1 aggregate | 13.695 M/s | **0.515x of a single producer** |
| per-producer at 4P | 3.42 M/s | 12.9% of solo → ~7.8x slowdown each |

Adding producers *reduces* aggregate throughput below one producer. This is
textbook cache-line ping-pong on the shared `reserved_tail` CAS: every failed
`compare_exchange_weak` re-reads a line another core just dirtied, and the retry
storm costs more than the payload work.

Hypothesis: inserting a bounded backoff (a few `cpu_relax()` on CAS failure) lets
the winning core drain its store buffer and publish the new tail before the
losers re-read, cutting the coherence storm. This is a ~15-line change versus the
Phase 4 ring/ABI rewrite.

## Goal

Recover unbatched 4P/1C aggregate throughput toward the 1P/1C ceiling
(26.6 M/s), i.e. push scaling efficiency from 0.51x toward >=0.8x, **without**
regressing the already-winning batch=16 path or the 1P/1C path.

Exit decision:
- If backoff lifts 4P batch=1 aggregate to >=~21 M/s (0.8x of 1P) → Phase 4 is
  unnecessary for this workload; stop.
- If backoff only marginally helps (<0.65x) → the contention is structural, and
  Phase 4 (independent per-producer rings) is justified.

## Scope

Two hot CAS loops, both currently spin with no backoff:

- `src/core/channel/mpsc.hpp:173-200` — `MpscChannel::Tx::claim()`
- `src/core/channel/shared_mpmc.hpp:149-173` — `SharedMpmcChannel::Tx::claim()`

The batch variants share the same loops (`claim_batch`), so they inherit the
backoff automatically.

Out of scope: SPSC (single producer, no CAS contention), consumer side, wait
strategy, public API, control-block ABI.

## Design

A tiny header-only backoff helper, tried under `wait/`. Truncated exponential:
spin `cpu_relax()` a growing number of times per consecutive CAS failure, capped
so a genuinely contended producer never sleeps or yields the core (we are on
pinned cores; yielding would hand the slot to no one).

`src/core/wait/cas_backoff.hpp` (new):

```cpp
#pragma once

#include <cstdint>

#include "core/wait/cpu_relax.hpp"

namespace salias::wait {

// Truncated exponential spin backoff for contended CAS loops.
// Reset on success; step() after each failed compare_exchange.
// Never yields or sleeps: callers run on pinned cores, so the winner must be
// left free to publish, not descheduled.
class CasBackoff {
 public:
  void step() noexcept {
    const std::uint32_t spins = current_;
    for (std::uint32_t i = 0; i < spins; ++i) {
      cpu_relax();
    }
    if (current_ < kMaxSpins) {
      current_ <<= 1;
    }
  }

  void reset() noexcept { current_ = kInitialSpins; }

 private:
  static constexpr std::uint32_t kInitialSpins = 1;
  static constexpr std::uint32_t kMaxSpins = 64;  // tune on Tencent
  std::uint32_t current_ = kInitialSpins;
};

}  // namespace salias::wait
```

### MPSC integration (`mpsc.hpp`)

The backoff object is local to a single `claim()` call — it must NOT persist
across calls, because a fresh claim starts uncontended. Insert one only on the
CAS-failure path; the success path returns immediately with no added cost.

```cpp
    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept {
      // ... unchanged capacity/need checks ...
      auto reserved_tail = std::atomic_ref<std::uint64_t>(channel_->reserved_tail_.value)
                               .load(std::memory_order_acquire);
      wait::CasBackoff backoff;  // NEW: local, reset per claim
      for (;;) {
        // ... unchanged capacity check + backpressure branch ...
        const std::uint64_t next_tail = reserved_tail + need;
        if (std::atomic_ref<std::uint64_t>(channel_->reserved_tail_.value)
                .compare_exchange_weak(reserved_tail, next_tail, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
          // ... unchanged header write + Claim return ...
        }
        backoff.step();  // NEW: only reached on CAS failure; reserved_tail was reloaded by CAS
      }
    }
```

Note: `compare_exchange_weak` already writes the current value back into
`reserved_tail` on failure, so the retry re-computes `next_tail` from the fresh
value. `backoff.step()` runs *after* that, delaying the next attempt.

### MPMC integration (`shared_mpmc.hpp`)

Identical placement in `SharedMpmcChannel::Tx::claim()` — declare
`wait::CasBackoff backoff;` before the loop, call `backoff.step()` at the end of
the loop body (only reached on CAS failure). The `min_subscriber_head` branch is
unchanged.

Both `claim_batch` loops get the same treatment (declare local backoff, step on
CAS failure).

## Why it is safe

- **No correctness change.** Backoff only inserts `cpu_relax()` delays between
  failed CAS attempts. The reservation protocol, generation gating, and
  commit-visibility ordering are untouched. A consumer still only observes a
  frame after its `FLAG_COMMITTED` release-store.
- **No deadlock/livelock.** Every producer still makes progress; backoff only
  reorders *when* losers retry, and is bounded (`kMaxSpins`). One winner always
  advances `reserved_tail` per round.
- **No sleeping/yielding.** On pinned cores, yielding would idle a core with no
  one to run; pure spin-relax keeps the winner free to publish.
- **Batch path preserved.** batch=16 rarely contends (16x fewer CAS), so backoff
  almost never triggers there — the winning path stays at its measured speed.

## Tuning knobs

`kMaxSpins` (start 64) and the growth factor are the only tunables. Sweep on
Tencent: 16 / 32 / 64 / 128. Too low → contention storm persists; too high →
producers over-delay and aggregate drops. Record the sweep.

## Validation

1. `cmake --build build && ctest --test-dir build --output-on-failure` — must
   stay 62/62.
2. ASan/UBSan build green (touches atomics):
   `cmake -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined"`.
3. New unit test `CasBackoff_ResetAndCapGrowth` (pure logic: step growth caps at
   kMaxSpins, reset restores initial).
4. Existing MPSC/MPMC channel tests unchanged and passing (proves no protocol
   regression).

## Benchmark protocol (Tencent, pinned)

Re-run the exact A/B harness so numbers are comparable to this round:
10M msgs/producer, 5 measured rounds, 64B payload, 4 MiB capacity/term, pinned.

Required comparisons:

| Run | Purpose |
| --- | ------- |
| 4P/1C batch=1 normal, before vs after | primary: did negative scaling improve? |
| 4P/1C batch=1 huge2m, before vs after | secondary |
| 1P/1C normal, before vs after | guard: backoff must NOT regress uncontended |
| 4P/1C batch=16 normal, before vs after | guard: batch path must NOT regress |
| MPMC 4P/2C batch=1, before vs after | MPMC contention improvement |
| kMaxSpins sweep 16/32/64/128 on 4P/1C batch=1 | pick the knob |

Record all in `doc/benchmarks/` with the raw log paths.

## Decision gate (drives Phase 4 go/no-go)

| 4P/1C batch=1 aggregate after backoff | Verdict |
| ------------------------------------- | ------- |
| >= ~21 M/s (>=0.8x of 1P) | Backoff sufficient. **Do not** build Phase 4. |
| ~17–21 M/s (0.65–0.8x) | Partial. Consider Phase 4 only if the unbatched latency-sensitive path is a real target. |
| < ~17 M/s (<0.65x) | Structural contention. **Phase 4 justified.** |

## Non-Goals

- No change to the batch API or public surface.
- No Phase 4 work in this plan — this experiment decides whether Phase 4 is even
  needed.
- No huge2m-regression fix here (separate perf-driven investigation).

## Steps

1. Add `src/core/wait/cas_backoff.hpp` + unit test.
2. Wire backoff into `mpsc.hpp` and `shared_mpmc.hpp` claim/claim_batch loops.
3. `ctest` + sanitizer green.
4. Tencent pinned re-run per the protocol table; sweep `kMaxSpins`.
5. Fill the decision gate; update the design doc's bottleneck priority with the
   result (backoff sufficient → close B1; else → promote Phase 4).
