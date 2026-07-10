# Throughput Optimization — Concrete Code Plan

Companion to `2026-07-08-throughput-optimization-design.md`. This file holds the
actual code changes per phase, so implementation is a mechanical apply-and-test
loop. Line references are against the current tree.

Every snippet keeps the existing invariant: a consumer only ever reads a frame
whose `FLAG_COMMITTED` bit and generation match. None of these changes weaken
that gate.

---

## Phase 1 — Cache `min_subscriber_head` (MPMC, targets B2)

### Problem

`shared_mpmc.hpp:150-157` calls `min_subscriber_head()` on every backpressure
branch, and `min_subscriber_head` (`shared_mpmc.hpp:98-120`) atomically loads
*every* subscriber slot each call. Near-full rings hit this branch on almost
every claim, turning a hot path into an O(N) cross-cache-line scan.

### Change

Add a coarse refresh cadence so the producer reuses its cached minimum for a run
of claims, and only rescans when the cache is stale *and* insufficient. The
cached value is always a lower bound refreshed from real slot loads, so a
stale-high cache can only cause a spurious `BackPressured` return — never
corruption or overwrite.

`src/core/channel/shared_mpmc.hpp`, in `class Tx`:

```cpp
  private:
    SharedMpmcChannel* channel_;
    std::uint64_t cached_min_head_ = 0;
    std::uint32_t refresh_countdown_ = 0;  // NEW

    static constexpr std::uint32_t kMinHeadRefreshInterval = 64;  // NEW
```

Rewrite the capacity-miss branch in `claim()` (`shared_mpmc.hpp:149-157`):

```cpp
      auto reserved_tail =
          std::atomic_ref<std::uint64_t>(*channel_->reserved_tail_).load(std::memory_order_acquire);
      for (;;) {
        // Fast path: reuse cached minimum without scanning subscriber slots.
        if (!detail::mpsc_has_capacity(channel_->capacity(), reserved_tail, cached_min_head_,
                                       need)) {
          // Cache says no room. Force a rescan of true subscriber heads before
          // returning backpressure — the cache is a lower bound and may be stale.
          cached_min_head_ = channel_->min_subscriber_head(reserved_tail);
          refresh_countdown_ = kMinHeadRefreshInterval;
          if (!detail::mpsc_has_capacity(channel_->capacity(), reserved_tail, cached_min_head_,
                                         need)) {
            return std::unexpected(flow::FlowError::BackPressured);
          }
        } else if (refresh_countdown_ == 0) {
          // Periodic refresh so a lagging subscriber is observed promptly even
          // when the ring is not near-full.
          cached_min_head_ = channel_->min_subscriber_head(reserved_tail);
          refresh_countdown_ = kMinHeadRefreshInterval;
        } else {
          --refresh_countdown_;
        }

        const std::uint64_t next_tail = reserved_tail + need;
        if (std::atomic_ref<std::uint64_t>(*channel_->reserved_tail_)
                .compare_exchange_weak(reserved_tail, next_tail, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
          // ... unchanged commit-header + Claim return ...
        }
      }
```

### Why it is safe

- `cached_min_head_` is only ever assigned from `min_subscriber_head()`, which
  reads live slots. It is a lower bound on the true minimum at scan time; heads
  only advance, so a cached value can only *under*-estimate available room.
- Under-estimating room → an occasional spurious `BackPressured`. The caller
  retries; no frame is ever overwritten before its subscriber releases it.
- The CAS still publishes the reservation; the scan cadence never touches the
  reservation protocol.

### Tests to add

- `SharedMpmcCachedMinHead_LaggingSubscriberEventuallyObserved`: one subscriber
  stops releasing; assert producer sees backpressure within
  `kMinHeadRefreshInterval` claims (bound, not exact).
- `SharedMpmcCachedMinHead_NoOverwriteUnderRefreshWindow`: fill ring, stall a
  subscriber, drive `kMinHeadRefreshInterval * 4` claims, assert no committed
  frame observed by the stalled subscriber is overwritten.

Run: `cmake --build build && ctest --test-dir build --output-on-failure`.

---

## Phase 2 — Control block layout hardening (targets B5)

### Problem

`channel.cpp:63-84` places the hot producer/consumer words on adjacent 64B
lines. On this microarchitecture, adjacent-line prefetch and coherence traffic
can still couple them under sustained 4P CAS.

### Change

Move hot words to a 128B stride (line + guard line). Bump `kNamedVersion` because
the on-shm control ABI changes; `connect_*` already rejects version mismatch at
`channel.cpp:920`, so old and new binaries fail closed instead of corrupting.

`src/salias/src/channel.cpp`:

```cpp
inline constexpr std::uint32_t kNamedVersion = 2;  // was 1: control layout changed
inline constexpr std::size_t kCacheLineGuard = 128;

struct alignas(kCacheLineGuard) NamedControl {
  std::uint32_t magic = 0;
  std::uint32_t version = 0;
  std::uint32_t mode = 0;
  std::uint32_t flags = 0;
  std::uint64_t capacity = 0;
  std::uint64_t record_size = 0;
  std::uint32_t ready = 0;
  std::uint32_t wait_word = 0;
  std::byte pad[kCacheLineGuard - 40]{};

  alignas(kCacheLineGuard) std::uint64_t producer_pos = 0;
  std::byte producer_pad[kCacheLineGuard - sizeof(std::uint64_t)]{};

  alignas(kCacheLineGuard) std::uint64_t consumer_pos = 0;
  std::byte consumer_pad[kCacheLineGuard - sizeof(std::uint64_t)]{};

  alignas(kCacheLineGuard) std::uint32_t subscriber_count = 0;
  std::byte subscriber_count_pad[kCacheLineGuard - sizeof(std::uint32_t)]{};

  alignas(kCacheLineGuard) std::array<NamedPositionSlot, kNamedMpmcMaxSubscribers>
      subscriber_heads{};
};
```

Update the static asserts (`channel.cpp:80-85`) to the new alignment, and confirm
`sizeof(NamedControl) <= kControlSize` (4096) still holds:
128 (prefix) + 128 (producer) + 128 (consumer) + 128 (count) + 8*64 (heads) = 1024
bytes — well under 4096, so `NamedPositionSlot` (64B) can stay at 64B or also go
to 128B if a follow-up scan shows head-line coupling.

```cpp
static_assert(alignof(NamedControl) == kCacheLineGuard);
static_assert(offsetof(NamedControl, producer_pos) % kCacheLineGuard == 0);
static_assert(offsetof(NamedControl, consumer_pos) % kCacheLineGuard == 0);
static_assert(offsetof(NamedControl, subscriber_count) % kCacheLineGuard == 0);
static_assert(sizeof(NamedControl) <= kControlSize);
```

### Why it is safe

Layout-only change. The version bump makes cross-binary mismatch fail closed via
the existing `VersionMismatch` path. No hot-path logic changes.

### Tests to add

- Existing named-channel tests must pass unchanged (they construct both ends in
  the same binary, so version matches).
- `NamedControlVersionMismatch_Rejected`: hand-write a control block with
  `version = 1`, assert `connect()` returns `Error::VersionMismatch`.

---

## Phase 3 — Batch reservation API (targets B1, B4)

### Problem

One CAS per message on `reserved_tail` (B1) and four header touches per message
(B4). Batching amortizes both.

### New types

`src/core/flow/producer.hpp`:

```cpp
struct BatchClaim {
  std::span<std::byte> region;   // contiguous span covering frame_count frames
  std::uint64_t start_pos = 0;
  std::uint32_t frame_len = 0;   // per-frame stride (fixed within a batch)
  std::uint32_t frame_count = 0;
};
```

### Channel API (shown for MPSC; MPMC identical modulo `min_subscriber_head`)

`src/core/channel/mpsc.hpp`, in `class Tx`:

```cpp
    using BatchClaimResult = std::expected<flow::BatchClaim, flow::FlowError>;

    // Reserve space for up to max_frames frames of equal payload_len with a
    // single CAS. Returns the largest batch that fits (>=1) or BackPressured.
    BatchClaimResult claim_batch(std::uint32_t payload_len,
                                 std::uint32_t max_frames) noexcept {
      if (channel_ == nullptr || channel_->capacity() == 0 || max_frames == 0) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }
      const std::size_t per = frame::frame_len(payload_len);
      if (per > channel_->capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto reserved_tail = std::atomic_ref<std::uint64_t>(channel_->reserved_tail_.value)
                               .load(std::memory_order_acquire);
      for (;;) {
        // How many frames fit right now?
        std::uint64_t used = reserved_tail - cached_head_;
        std::uint64_t free_bytes =
            used <= channel_->capacity() ? channel_->capacity() - used : 0;
        std::uint32_t fit = static_cast<std::uint32_t>(free_bytes / per);
        if (fit == 0) {
          cached_head_ = std::atomic_ref<std::uint64_t>(channel_->consumer_pos_.value)
                             .load(std::memory_order_acquire);
          used = reserved_tail - cached_head_;
          free_bytes = used <= channel_->capacity() ? channel_->capacity() - used : 0;
          fit = static_cast<std::uint32_t>(free_bytes / per);
          if (fit == 0) {
            return std::unexpected(flow::FlowError::BackPressured);
          }
        }
        const std::uint32_t n = fit < max_frames ? fit : max_frames;
        const std::uint64_t span_bytes = static_cast<std::uint64_t>(per) * n;
        const std::uint64_t next_tail = reserved_tail + span_bytes;

        if (std::atomic_ref<std::uint64_t>(channel_->reserved_tail_.value)
                .compare_exchange_weak(reserved_tail, next_tail, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
          // Write all N uncommitted headers now; payload written by caller.
          for (std::uint32_t i = 0; i < n; ++i) {
            const std::uint64_t pos = reserved_tail + static_cast<std::uint64_t>(per) * i;
            const std::uint32_t meta =
                detail::mpsc_meta(pos, channel_->capacity(), false);
            detail::write_uncommitted_header(channel_->ring_, pos, payload_len, meta);
          }
          return flow::BatchClaim{
              .region = channel_->ring_.slice_mut(reserved_tail, span_bytes),
              .start_pos = reserved_tail,
              .frame_len = static_cast<std::uint32_t>(per),
              .frame_count = n,
          };
        }
      }
    }

    // Commit an entire batch in position order so the consumer's per-frame
    // FLAG_COMMITTED gate stays valid (frame i visible only after i-1).
    void commit_batch(const flow::BatchClaim& batch) noexcept {
      for (std::uint32_t i = 0; i < batch.frame_count; ++i) {
        const std::uint64_t pos =
            batch.start_pos + static_cast<std::uint64_t>(batch.frame_len) * i;
        const std::uint32_t meta =
            detail::mpsc_meta(pos, channel_->capacity(), true);
        detail::store_meta_release(channel_->ring_, pos, meta);
      }
      std::atomic_ref<std::uint32_t>(channel_->wait_word_)
          .fetch_add(1, std::memory_order_release);
      channel_->wait_.wake(&channel_->wait_word_);
    }
```

### Facade wiring

`src/salias/src/channel.cpp`: add `Publisher::offer_batch(std::span<const
std::byte> payloads[], count)` or a strided variant, dispatching through the
existing `std::visit`. Single-message `offer()` stays as the `max_frames == 1`
path and is unchanged.

### Why it is safe

- The consumer is untouched: it still walks frames in position order and gates on
  per-frame `FLAG_COMMITTED` + generation. Because `commit_batch` release-stores
  commit flags in ascending position order, the consumer can never see frame `i`
  committed while frame `i-1` is not.
- A crash mid-batch leaves later frames uncommitted; the consumer stops at the
  first uncommitted frame exactly as today. No orphan reads.
- Backpressure semantics preserved: a batch of `n >= 1` is only returned when `n`
  frames provably fit; otherwise `BackPressured`.

### Tests to add

- `MpscBatch_SingleCasReservesN`: assert one `claim_batch` yields a contiguous
  region for N frames and consumer reads exactly N in order.
- `MpscBatch_PartialFitReturnsSmallerBatch`: ring with room for 3, request 8 →
  batch of 3.
- `MpscBatch_CrashMidBatchNoOrphanRead`: commit only frames 0..k of an N batch,
  assert consumer stops at k+1.
- `MpscBatch_FullThenBackpressure`: no room → `BackPressured`.
- Mirror all four for `SharedMpmcChannel` (with `min_subscriber_head` in the fit
  computation).
- Run under ASan/UBSan: `cmake -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined"`.

---

## Phase 4 (stretch) — Rotating term buffers (targets B1)

Deferred. Sketch only, to be designed after Phases 1–3 are measured.

- Replace the single `MagicRing` with `std::array<MagicRing, kTerms>` (kTerms=3)
  plus an atomic `active_term` index in the control block.
- Producers CAS a per-term tail; the active term advances when a term fills.
- Consumer tracks `(term, position)` and rolls to the next term on term boundary.
- This is a control-ABI rewrite (another `kNamedVersion` bump) and needs the full
  named-mode test matrix re-run. Do not start before Phases 1–3 numbers justify
  the structural cost.

---

## Apply Order & Gate

1. Phase 1 → build → ctest → pinned MPMC benchmark → record delta.
2. Phase 2 → build → ctest (incl. version-mismatch test) → benchmark → record.
3. Phase 3 → build → ctest + ASan/UBSan → benchmark both modes → record.
4. Re-evaluate whether Phase 4 is worth it against remaining gap.

Each gate must be green before the next phase. Record every before/after number
in `doc/benchmarks/` with the exact pinned-harness config.
