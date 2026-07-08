# SPMC/MPMC Hot Path Design

Date: 2026-07-07

## Context

The stricter salias/Aeron release comparison showed a stable SPSC advantage for
salias, but SPMC and the current MPMC composed topology lagged Aeron in the
2-vCPU Colima VM run. The result should not be read as a final SPMC/MPMC product
claim because the comparison currently exercises different hot paths:

- Aeron producers use `ExclusivePublication::tryClaim`.
- Aeron consumers use `Subscription::poll(handler, fragment_limit)`, batching up
  to 64 fragments per call.
- salias producers use the public `Publisher::offer`, which copies the user
  buffer into the ring.
- salias consumers use public single-message `Subscriber::try_recv` and
  `release`, which revisit the type-erased facade on every message.
- salias MPMC is currently a composed topology: one reliable Broadcast channel
  per producer, with every consumer polling every producer shard. It is not a
  native single-channel MPMC fanout.

SPMC and MPMC are common production shapes, so they need first-class hot paths
before salias can make serious comparison claims.

## Design Direction

Implement the optimizations in narrow layers before changing the channel model.
This keeps each benchmark delta attributable.

### Phase 1: Public Batch Poll

Add a public `Subscriber::poll(max_messages, handler)` API. It should visit the
underlying channel variant once, create the core receiver once, and consume up to
`max_messages` available messages in one call. The handler receives a borrowed
`Message` view that is valid only during the callback; `poll` releases each
message automatically after the handler returns.

This aligns the public salias consumer path with Aeron's `poll(fragment_limit)`
shape and removes the per-message public facade dispatch from benchmark loops.

### Phase 2: Public Zero-Copy Claim/Commit

Add a public producer claim API that exposes an in-ring writable payload span and
a commit operation. This aligns salias with Aeron's `tryClaim` and avoids the
extra `offer` copy in throughput benchmarks.

The public claim object must be move-only and must make ownership explicit: a
claim belongs to one publisher, references one reserved ring range, and must be
committed at most once. Backpressure and oversized messages continue to use the
existing `Result`/`Error` surface.

### Phase 3: Benchmark Parity

Extend `salias_bench_compare` so salias can run with:

- `--poll-limit N` for batch consumer polling.
- `--publish-api offer|claim` for producer path selection.

The Aeron side already has `--fragment-limit`; use matching limits when reporting
comparisons.

### Phase 4: Native MPMC Fanout

After the public hot paths are comparable, design a native single-channel MPMC
fanout. This should not be mixed with the earlier phases because it changes the
channel topology, producer arbitration, consumer progress model, and benchmark
interpretation at the same time.

## Initial Acceptance Criteria

- Public `Subscriber::poll` supports SPSC, MPSC, Broadcast, Bulk, and named
  SPSC/MPSC/MPMC.
- `poll` returns the number of consumed messages and never calls the handler when
  no message is available.
- `poll` with `max_messages == 0` returns 0.
- Callback payload views are valid during the callback and are released by the
  time `poll` returns.
- Existing single-message `try_recv`/`release` behavior remains unchanged.
- Release build and CTest pass in the Linux VM.
- Benchmark output clearly reports whether salias used single-message or batch
  polling.

## Risks

- Auto-release callback semantics are different from `try_recv`/`release`; the
  API comments must make the borrowed lifetime explicit.
- A public template callback API can become expensive if it uses heap-allocating
  type erasure. The implementation should use a non-owning callback trampoline.
- Batch poll improves the current composed MPMC benchmark; named MPMC fanout is
  handled by the cross-process MPSC/MPMC plan.
