# Subscription Capabilities: G1-G9

Source: `gts-ai/docs/subscription-model.md`, the G1-G9 gap table. Only this
salias worktree is writable for this task. Preserve gts-ai's uncommitted files.
Each capability has its own local commit, tests, API documentation and changelog
entry. No version tags, pushes or changes to the installed dependency copy.

## Requirements and Evidence

| Item | Required Outcome | Evidence / Status |
| --- | --- | --- |
| Ringbuffer | Static element capacity and configurable percentage, default 90%, drop-oldest | `6ee20d2`; `test/ring/ringbuffer_test.cpp` |
| G1 | Reusable subscriber slots, deregistration, crash reclamation, safe late join/restart and explicit exhaustion | Pending. Registration generations must protect old endpoints and borrowed payloads; allocation failure must roll back. |
| G2 | Process liveness and connected subscriber count | `6b69e17`; PID/start-token checks and cross-process API tests |
| G3 | Bounded IPC drop-oldest with Publisher/Subscriber endpoints | `afe8ab1`, `501f502`; LatestFanout frame/lease tests and six benchmarks in `6a1185e` |
| G4 | Explicit latest seek, exact gap counters, and a real Lagged reporting path | `try_recv_checked`, `lagged_messages`, `skipped_messages`, `skip_to_latest` on the existing endpoints; no implicit loss in reliable modes. Cross-process and wraparound tests pass. |
| G5 | Per-subscriber reliable or drop-oldest policy, without discarding reliable readers' data | Pending. Build on G1 generations and G4 loss reporting; protect both single and batched loans. |
| G6 | Keyed latest-value IPC primitive, bounded memory, real cross-process conflation | Pending. Key equality/collisions and updates to a borrowed value need explicit semantics, not an application-side map. |
| G7 | Priority-aware delivery and coordinated multi-channel subscription | Pending. Define starvation/fairness and per-stream ordering, with backpressure independent across priorities. |
| G8 | Message-count retention/flow limits alongside byte capacity | Pending. Enforce whole-message counts for variable sizes, batches and wraparound; template-configurable latest N. |
| G9 | TTL-aware IPC expiration, expired frames release capacity without dropping live reliable data | Pending. Explicit transport metadata and clock domain; no hard-coded business payload parsing. |

## Implementation Order

G2, G3 and G4 are committed. Finish G1, G5, G8, G9, G6 and G7. The order
puts gap observability before lifecycle/policy work and shares the same pinning
contract throughout. Preserve the existing reliable modes unless a caller
explicitly opts into a lossy policy. Do not treat a local wrapper, docs alone,
or an unused API declaration as completion of an IPC capability.

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
