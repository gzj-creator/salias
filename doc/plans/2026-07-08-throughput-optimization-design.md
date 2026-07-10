# Throughput Optimization Design (vs Aeron IPC)

## Goal

Close the measured throughput gap between salias and Aeron IPC on the Tencent
CVM baseline. Target: shrink the gap from the current 13–23% down to 5–10% under
a fair, CPU-pinned comparison.

## Baseline (Tencent CVM, do not treat as final whitepaper numbers)

Environment: Tencent CVM, x86_64, 4 vCPU, Intel Xeon Platinum 8255C, KVM.
Build: Release + LTO + `-march=native`. 3 warmup + 10 measured rounds,
1,000,000 msgs/producer, 64B payload. `ctest` 51/51 passed.

Throughput in M msg/s:

| Scenario    | Object        | avg publish | avg delivery | ratio vs Aeron |
| ----------- | ------------- | ----------- | ------------ | -------------- |
| MPSC 4P/1C  | salias normal | 14.373      | 14.373       | 0.87x          |
| MPSC 4P/1C  | salias huge2m | 14.043      | 14.043       | 0.85x          |
| MPSC 4P/1C  | Aeron IPC     | 16.525      | 16.525       | 1.00x          |
| MPMC 4P/2C  | salias normal | 10.980      | 21.961       | 0.77x          |
| MPMC 4P/2C  | salias huge2m | 11.630      | 23.260       | 0.81x          |
| MPMC 4P/2C  | Aeron IPC     | 14.293      | 28.586       | 1.00x          |

Caveats baked into these numbers: 4 vCPU is an overload scenario for 4P/2C +
Aeron driver; no per-process CPU pinning; no Aeron tuning config; 1GiB huge page
could not be reserved at runtime on this VM (huge1g all SKIP).

## Bottleneck Analysis

Ranked by estimated contribution to the gap. Percentages are estimates from code
inspection, not yet confirmed by `perf`.

### B1. Single-point CAS contention on `reserved_tail` (est. 10–12%)

Every producer competes for one atomic `reserved_tail` via
`compare_exchange_weak` on the claim hot path
(`mpsc.hpp:187`, `shared_mpmc.hpp:160`). Under 4 producers the cache line holding
`reserved_tail` bounces between cores on every contended claim, and failed CAS
loops re-read and retry. This is the dominant cost in both MPSC and MPMC.

Aeron mitigates the equivalent cost with rotating term buffers, spreading
reservation pressure across multiple backing regions.

### B2. `min_subscriber_head` O(N) scan on backpressure (MPMC only, est. 8–10%)

`shared_mpmc.hpp:98-120` recomputes the minimum subscriber head by atomically
loading every subscriber slot each time a producer hits the backpressure branch
(`shared_mpmc.hpp:152`). Each slot is a separate cache line, so the scan is a
cross-line atomic-load fan-out executed on the producer hot path. This is unique
to MPMC and explains why the MPMC gap (23%) is wider than MPSC (13%).

### B3. CPU overload + scheduling (est. 3–5%)

6 app threads (4P + 2C) plus the Aeron MediaDriver thread contend for 4 vCPUs
under KVM. The `SpinPause` wait strategy burns cycles that a descheduled peer
needs, amplifying the overload. This is partly a benchmark-fairness artifact, not
a pure salias defect.

### B4. Per-message frame header + double metadata touch (est. 2–3%)

64B payloads carry an 8B header (4B len + 4B meta), a 12.5% framing overhead. The
producer writes the header uncommitted (`write_uncommitted_header`), then release-
stores the committed meta (`store_meta_release`); the consumer acquire-loads meta
then reads len. Four separate touches of the header region per message.

### B5. Control block layout / false sharing (est. 1–3%)

`NamedControl` (`channel.cpp:63-84`) 64B-aligns `producer_pos`, `consumer_pos`,
`subscriber_count`, and each subscriber head. Under sustained 4P CAS, adjacent
64B lines can still suffer coherence traffic; 128B separation is the safer
guard on this microarchitecture. The 8 subscriber-head cache lines are also read
in bulk by B2.

## Optimization Plan

Phased so each phase is independently shippable and measurable. Every phase must
keep `ctest` green and be measured on the CPU-pinned harness (see Benchmark
Fairness below) before the next phase starts.

### Phase 0 — Fair measurement harness (prerequisite, no perf change)

Goal: make the comparison trustworthy so later phases have a real signal.

- Add CPU pinning to the compare tool: pin each producer/consumer to a distinct
  physical core; pin the Aeron MediaDriver to its own core; keep salias and Aeron
  on the same core set.
- Align Aeron term buffer length with the salias ring capacity, and record the
  exact Aeron config used.
- Capture `perf stat` (cache-misses, L1-dcache-load-misses) and a flamegraph for
  both MPSC and MPMC runs, checked into `doc/benchmarks/`.

Exit criteria: reproducible per-core-pinned numbers; a flamegraph that confirms
B1/B2 are the top self-time frames.

### Phase 1 — Cache `min_subscriber_head` on the producer (MPMC, targets B2)

Replace the per-backpressure O(N) scan with a producer-local cached minimum that
is only refreshed when the cached value proves insufficient (already the pattern
for `cached_min_head_`), plus a bounded refresh counter so a lagging subscriber
is still observed promptly.

- Keep `cached_min_head_` as the fast path; on capacity-miss, refresh at most once
  per claim (current code already does this — the change is to avoid rescanning
  when the cached value still satisfies `need`).
- Add a coarse refresh cadence so producers don't rescan every message when the
  ring is near-full and every claim hits the branch.

Risk: low. Correctness invariant unchanged — the cached min is always <= true
min at refresh time, and a stale-high cache only ever causes a spurious
backpressure return, never data corruption.

Expected: 4–6% on MPMC.

### Phase 2 — Control block layout hardening (targets B5)

- Separate the true producer/consumer hot words to 128B stride (line + guard).
- Keep subscriber heads contiguous but document that B2's caching (Phase 1)
  removes them from the hot path.

Risk: low (layout only; ABI-versioned via existing `kNamedVersion`). If the
on-disk control ABI changes, bump `kNamedVersion` and reject mismatches in the
`connect_*` paths (already validated at `channel.cpp:920`).

Expected: 1–3% both modes.

### Phase 3 — Batch reservation API (targets B1, B4)

Add a batch claim that reserves space for multiple messages with a single CAS,
amortizing both the contention (B1) and the per-message header round-trips (B4).

```cpp
struct BatchClaim {
  std::span<std::byte> region;  // contiguous, spans multiple frames
  std::uint64_t start_pos;
  std::uint32_t frame_count;
};

// One CAS reserves the whole region; caller writes N frames then commits once.
flow::Producer::BatchClaimResult claim_batch(std::uint32_t frame_len,
                                             std::uint32_t max_frames) noexcept;
```

- One CAS per batch instead of per message directly attacks B1.
- A single commit fence per batch reduces meta touches (B4).
- Publisher/Subscriber facade in `channel.cpp` gains a batch offer path; the
  single-message path stays as a `max_frames == 1` special case.

Risk: medium. Batching changes the commit-visibility granularity — a consumer
must not observe a partially written batch. Design: reserve uncommitted, write
all frames, then release-store commit flags in position order (or a single batch
commit marker) so the consumer's existing per-frame `FLAG_COMMITTED` gate still
holds. Requires new tests for partial-batch backpressure and crash-mid-batch
visibility.

Expected: 5–8% both modes.

### Phase 4 (stretch) — Rotating term buffers (targets B1)

Mirror Aeron's structural fix: N backing regions with an atomically selected
active term, so reservation pressure is spread across N cache lines instead of
one. This is the highest-effort, highest-payoff change and is deferred until
Phases 1–3 are measured, because it is a larger structural rewrite of the ring
and its cross-process control ABI.

Risk: high (ring + control ABI rewrite, new tests across all named modes).
Expected: additional 5–10% under high producer counts.

## Benchmark Fairness

Later comparison runs must, at minimum:

- Pin each process to a dedicated physical core (`taskset -c`).
- Pin the Aeron MediaDriver to its own core.
- Use matched buffer sizes (Aeron term buffer == salias ring capacity).
- Report `perf stat` counters alongside throughput.
- State explicitly whether huge pages were actually backed (not requested).

## Non-Goals

- No claim of beating Aeron; the goal is closing the gap to 5–10% on this VM.
- No 1GiB huge page work until a host that can reserve them at runtime is
  available.
- No change to the public `salias::Config` surface in Phases 0–2.

## Validation

- `ctest` must stay 51/51 (or grow) after every phase.
- Sanitizer build (ASan/UBSan) must pass for Phases 1–4, since all touch atomics
  and cross-process shared memory.
- Each phase records before/after throughput on the pinned harness in
  `doc/benchmarks/`.

## Sequencing Summary

| Phase | Targets    | Risk   | Est. gain    | Blocking on |
| ----- | ---------- | ------ | ------------ | ----------- |
| 0     | fairness   | none   | measurement  | —           |
| 1     | B2 (MPMC)  | low    | 4–6% MPMC    | 0           |
| 2     | B5         | low    | 1–3% both    | 0           |
| 3     | B1, B4     | medium | 5–8% both    | 0, tests    |
| 4     | B1         | high   | +5–10%       | 1–3 done    |
