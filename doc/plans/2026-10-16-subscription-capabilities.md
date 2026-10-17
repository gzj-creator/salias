# Subscription Capabilities: G1-G9

Source: `gts-ai/docs/subscription-model.md`, the G1-G9 gap table. Only this
salias worktree is writable for this task. Preserve gts-ai's uncommitted files.
Each capability has its own local commit, tests, API documentation and changelog
entry. No version tags, pushes or changes to the installed dependency copy.

## Requirements and Evidence

| Item | Required Outcome | Evidence / Status |
| --- | --- | --- |
| Ringbuffer | Static element capacity and configurable percentage, default 90%, drop-oldest | `6ee20d2`; `test/ring/ringbuffer_test.cpp` |
| G1 | Reusable subscriber slots, deregistration, crash reclamation, safe late join/restart and explicit exhaustion | Implemented: generation-checked RAII slots, stable reservation snapshots, OOM rollback, explicit `NoSubscriberSlots`, dead-process reclamation, including fork-inherited endpoint cleanup. Release CTest 124/124; ASan/UBSan 123/123; focused TSan 6/6; mcpp import test passed. |
| G2 | Process liveness and connected subscriber count | `6b69e17`; PID/start-token checks and cross-process API tests |
| G3 | Bounded IPC drop-oldest with Publisher/Subscriber endpoints | `afe8ab1`, `501f502`; LatestFanout frame/lease tests and six benchmarks in `6a1185e` |
| G4 | Explicit latest seek, exact gap counters, and a real Lagged reporting path | `try_recv_checked`, `lagged_messages`, `skipped_messages`, `skip_to_latest` on the existing endpoints; no implicit loss in reliable modes. Cross-process and wraparound tests pass. |
| G5 | Per-subscriber reliable or drop-oldest policy, without discarding reliable readers' data | Implemented in `f2aeedf`: LatestFanout supports independent Reliable/DropOldest slots; single and batch loans protect reliable readers. |
| G6 | Keyed latest-value IPC primitive, bounded memory, real cross-process conflation | Implemented as `LatestByKeyChannel<Key, Value, Capacity>`: exact key-byte equality, linear probing, explicit capacity exhaustion, per-subscriber snapshots and conflation counters, copied values, bounded event log with snapshot rescan on overrun, crash-released file locking and double-buffer commits. Ten focused Release/ASan/UBSan/TSan tests cover collisions, saturation, schema mismatch, event overrun, independent and fork-inherited IPC, same-process multi-handle concurrency, lock contention, key-version/event-sequence exhaustion and SIGKILL during partial writes. mcpp module/header mixed roundtrip passes. Release full suite 152/152; ASan/UBSan full suite 151/151; focused TSan suite passes. 64B publish/receive roundtrip median 13.0 us over three 0.5-second runs on 2026-10-17; Google Benchmark dependency reports DEBUG, so this is only a smoke measurement, not a calibrated baseline. |
| G7 | Priority-aware delivery and coordinated multi-channel subscription | Implemented as `PriorityPublisher<Mode...>` and move-only `PrioritySubscriber<Mode...>` over independent channels. Template order is high-to-low; publication touches only the selected stream. The subscriber uses work-conserving weighted round robin with weights N..1, preserves each stream's order, tags borrowed messages with their stream index and releases per stream; channel contention/loss does not prevent checking other streams. Seven focused tests cover 2:1 and 3:2:1 service, per-stream sequence, isolated saturation, LatestFanout lag isolation, outstanding borrow, poll release, cross-process publish and invalid/moved endpoints. mcpp module/header mixed IPC roundtrip passes. The 64B weighted roundtrip benchmark median is 0.122 us over three 0.5-second repetitions on 2026-10-17; Google Benchmark dependency reports DEBUG, so this is a smoke result only. Final Release 159/159, ASan/UBSan 158/158 and focused TSan 1/1 pass. |
| G8 | Message-count retention/flow limits alongside byte capacity | Implemented in `1d7a19f`: per-producer message limits, LatestFanout retained-count limits, partial batch handling and byte-window independence. |
| G9 | TTL-aware IPC expiration, expired frames release capacity without dropping live reliable data | Implemented: explicit per-frame monotonic deadline metadata; poll/recv skips expired prefixes and publishes progress before returning the next live borrowed frame. It never crosses that live frame, even if its TTL expires while borrowed. No polling means no background expiry cleanup. Release 142/142; ASan/UBSan 141/141; TSan API suite 1/1; mcpp module/header TTL IPC pass. |

## Implementation Order

Ringbuffer and G1-G9 are implemented. The order
puts gap observability before lifecycle/policy work and shares the same pinning
contract throughout. Preserve the existing reliable modes unless the transport
metadata says a frame has expired. Do not treat a local wrapper, docs alone, or
an unused API declaration as completion of an IPC capability.

The G1 registration roundtrip microbenchmark measured 205 ns with a persistent
subscriber and 22.2 us with per-iteration registration churn (five repetitions,
one second minimum per repetition). The executable is optimized, but the host
Google Benchmark library reports that it was built as DEBUG; treat these figures
as indicative only, not a calibrated baseline. This is not a long-duration stress run.

## Verification Gates

- Each item: configuration and API contracts, empty/exact/over-limit boundaries,
  partial batches, wraparound, borrow lifetime, independent endpoints and errors.
- Shared protocols: concurrent producers/readers under TSan, cross-process attach,
  normal exit, SIGKILL where relevant, generation/restart and OOM rollback.
- Run full Release and ASan/UBSan tests after shared-engine changes. Verify the
  mcpp module/header path without `tools/check_mcpp.py` or copying installed deps.
- Add focused pressure benchmarks when a new hot path is introduced. Short
  correctness runs are not long-duration stress or a calibrated performance baseline.
- Verify git status and hashes of gts-ai's dirty files before final completion.
- Completion requires direct evidence for all G1-G9 rows, not only passing tests
  for the subset already implemented. macOS and long-duration runs remain explicit
  verification limitations until executed.
