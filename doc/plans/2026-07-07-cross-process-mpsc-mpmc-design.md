# Cross-Process MPSC/MPMC Fanout Design

Date: 2026-07-07

## Goal

Support named cross-process MPSC and MPMC so salias can be compared against
Aeron IPC using real process-to-process communication instead of the current
in-process thread benchmark.

## Confirmed Semantics

MPMC follows Aeron IPC pub/sub fanout semantics:

- Multiple publisher processes may publish to one named channel.
- Multiple subscriber processes may subscribe to that same named channel.
- Every subscriber receives every message from every publisher.
- This is not a competing-consumer queue.

## Current Boundary

Named channels now support SPSC, MPSC, and MPMC fanout. The named control block
keeps the original SPSC/MPSC prefix and appends the MPMC subscriber allocation
counter plus fixed subscriber head table. The ring itself is mapped from a
shared-memory fd and reused directly by peers.

In-process MPSC already has the core producer arbitration model:

- Producers reserve disjoint byte ranges with a CAS on `reserved_tail`.
- Each producer writes an uncommitted header, fills payload, then commits by
  release-storing the metadata flag.
- The consumer reads in position order and does not skip uncommitted gaps.

That makes cross-process MPSC the smallest correct next step.

## Implementation Status

- Named MPSC create/connect is implemented with `SharedMpscChannel`.
- Named MPMC create/connect is implemented with `SharedMpmcChannel`.
- The public `Mode::Mpmc` value is currently supported for named channels.
  In-process `Mode::Mpmc` still returns `BadConfig`.
- The release comparison harness has not yet been switched to true salias
  multi-process IPC.

## Phase 1: Named Cross-Process MPSC

Extend the named control ABI so the existing `producer_pos` cache-line slot is
interpreted as:

- SPSC: producer tail.
- MPSC: reserved tail.

The existing `consumer_pos` slot remains the single consumer head. A new
`SharedMpscChannel` mirrors `MpscChannel` but stores positions in the named
control block instead of owning in-process `CacheAligned` fields.

`Channel::create(Config{name, Mode::Mpsc})` creates the named control/ring and
publishes mode metadata. `Channel::connect(name)` reads the mode from metadata
and constructs the correct shared channel variant.

Status: implemented.

## Phase 2: Named Cross-Process MPMC Fanout

Add a shared fanout control layout with:

- one shared `reserved_tail` for all producers,
- a fixed subscriber table,
- one head position per subscriber,
- a subscriber allocation counter,
- a wait word for poll/recv wakeups.

The data ring uses the same committed-frame ordering as MPSC. Each subscriber
reads independently from its own head. Producers calculate reusable capacity
from the minimum active subscriber head, so reliable fanout backpressures on the
slowest subscriber.

The initial implementation should use a fixed maximum subscriber count matching
the current Broadcast default, then make it configurable later only if needed.

Status: implemented with 8 fixed subscriber slots. Slots are allocated
monotonically and are not reclaimed yet.

## Phase 3: Cross-Process Benchmark

Replace the current salias/Aeron comparison for named modes with a true
multi-process benchmark:

- parent creates named channel,
- publisher children connect and publish,
- subscriber children connect and poll,
- parent measures from a shared start signal until all subscribers report the
  expected delivery count.

The Aeron side already uses an external media driver and IPC transport. The
salias side must use separate OS processes for a fair IPC comparison.

## Risks

- The named control ABI must remain validated before mapping ring capacity from
  peer-controlled metadata.
- MPSC/MPMC producer crash after reserve but before commit leaves a permanent
  gap. This matches the current in-process MPSC semantics and should be
  documented for production users.
- MPMC fanout subscriber slots are monotonic and not reclaimed until process
  exit cleanup exists.
- Benchmark results must label whether they are in-process or cross-process.
