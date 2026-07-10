# Changelog

## Maintenance Notes

This file records user-facing and engineering-significant changes before they
are released.

- Keep pending local commits under `## [Unreleased]`.
- When creating a release tag, move relevant entries into a version section
  formatted as `## [vX.Y.Z] - YYYY-MM-DD`.
- Group changes by `Added`, `Changed`, `Fixed`, `Docs`, or similar sections.
- Prefer concise behavioral summaries over mechanical file lists.

## [Unreleased]

### Added

- Channel publication flow-control window: a new `Config::publication_window`
  (bytes, `0` = full ring) caps how far a producer may lead its consumer at
  `min(ring_capacity, window)` in-flight bytes, letting steady-state queueing
  latency be driven down from "full ring" to "window size" without changing the
  ring geometry or wraparound. Plumbed through `hybrid_control.hpp`,
  `hybrid_mpsc.hpp`, and `shared_hybrid_mpsc.hpp` via a new
  `effective_capacity()` gate, and validated at named-channel create/connect
  (a non-zero window must fall in `[page, capacity]`).
- Consumer batch-receive hot path on both hybrid engines: a new
  `try_recv_run()` drains a single ring in a tight loop, while `consume()` /
  `flush_progress()` collapse per-message shared release-stores to a per-batch
  per-ring pair, and a cached `visible_producer_pos` drops cross-core
  acquire-loads from per-message to per-batch per-ring.
- Inlined L7 `Subscriber::poll` handler: the template path now batches into a
  32-entry buffer and runs the handler in-header, removing the per-message
  `.so` indirect call and flushing progress once per batch.
- `--publication-window` option on the `salias_ipc_compare` harness.
- Eight GTest cases covering window-bounded inflight, zero-window full-ring
  regression, batched flush reclaiming space, cached visible-position staging,
  and same-ring run draining / truncation for FIFO and Ordered.

### Changed

- Named IPC control block now carries `publication_window`; `kNamedVersion`
  raised `3 -> 4` (binary-incompatible protocol bump).

### Docs

- Updated the Tencent 4 vCPU Aeron comparison with flow-control-window and
  Phase A re-verification results (FIFO throughput lifted to ~85% of Aeron; a
  128 KiB window cuts FIFO p50 from 5.37 ms to 147 µs), plus the matching raw
  benchmark logs.

## [v1.0.0] - 2026-07-10

### Added

- Redesigned the channel layer onto a per-producer hybrid magic-ring engine
  (`hybrid_control.hpp`, `hybrid_mpsc.hpp`, `shared_hybrid_mpsc.hpp`) where each
  producer owns a private ring and the consumer merges by `global_seq`, replacing
  the single-ring CAS MPSC/MPMC designs.
- Exposed four public channel modes through `salias::Mode`
  (`FifoMpsc` / `FifoFanout` / `OrderedMpsc` / `OrderedFanout`): FIFO modes keep
  the producer hot path free of shared read-modify-write, ordered modes establish
  cross-producer global total order via a single `global_seq.fetch_add`.
- Added `salias::HugePage` (`None` / `Size2MB` / `Size1GB`) on the public
  `Config`, mapping to `memfd_create(MFD_HUGETLB)` in-process and
  hugetlbfs-backed named IPC rings.
- Added runnable MPSC/MPMC publisher/subscriber examples under `example/` with a
  CTest smoke script.
- Added sampled p50/p99 one-way latency reporting to the salias and Aeron IPC
  comparison tools, backed by a header-only logarithmic histogram helper
  (`latency_histogram.hpp`) and unit tests.
- Added frame sequence split/encode helpers (`frame/sequence.hpp`) carrying the
  24-bit in-header sequence plus side high bits.
- Added design and implementation plan docs under `doc/plans/` for the hybrid
  ring, dual-engine redesign, huge page support, named huge IPC, throughput /
  CAS-backoff optimization, example pub/sub, and IPC latency sampling, plus a
  Tencent CVM Aeron comparison report.
- Initialized the Linux-only C++20 CMake project structure for salias.
- Added the documented layered source layout under `src/`, covering platform,
  ring, frame, flow, wait strategy, channel, metrics, and public API modules.
- Implemented L0 double mapping and futex primitives, plus focused tests.
- Added initial SPSC ring/channel flow, metrics counters, public API wrappers,
  benchmark smoke target, sanitizer presets, and GTest coverage.
- Added L5 MPSC, reliable Broadcast, and Bulk channel implementations with
  focused GTest coverage for committed-gap ordering, independent subscribers,
  slow-subscriber backpressure, and single-frame large messages.
- Extended the in-process L7 `Channel` facade to create SPSC, MPSC, Broadcast,
  and Bulk modes through the public `Config::mode` setting.
- Added named SPSC driverless create/connect over POSIX shared memory, including
  peer-first ready waiting and fork-based end-to-end API coverage.
- Added L7 named connect rejection coverage for version mismatches and
  untrusted metadata before mapping peer-controlled ring capacity.
- Added a channel stress benchmark covering SPSC, MPSC, Broadcast, and Bulk
  in-process workloads.
- Added release comparison runners and an Aeron C++ IPC baseline report for
  SPSC, SPMC, and all-to-all MPMC pub/sub workloads.
- Added L0 support for double mapping an existing shared-memory file descriptor.
- Added read-only `CountersReader::open(path)` support for external metrics
  observers over mmap-backed counter files.
- Added the project design documentation set under `doc/`.
- Added public `Subscriber::poll` batch consumption and public
  `Publisher::try_claim` / `PublishClaim::commit` zero-copy publishing APIs.
- Added an SPMC/MPMC hot-path design and implementation plan under `doc/plans/`.
- Added named cross-process MPSC create/connect support over POSIX shared memory.
- Added named cross-process MPMC fanout support where every subscriber receives
  every publisher's messages over one shared ring.
- Added a true multi-process salias IPC vs Aeron IPC comparison harness for
  MPSC and MPMC workloads.

### Changed

- Rewrote the public `salias` layer (`channel.cpp`, `channel.hpp`, `config.hpp`,
  `message.hpp`, `publisher.hpp`, `subscriber.hpp`) onto the four-mode
  per-producer engine.
- Rewrote `README.md` to describe the converged per-producer engine and the four
  modes, trimming the stale per-layer implementation status.
- Migrated the build and public/core result types to C++23 `std::expected`,
  removing the custom platform result wrapper and the `tl-expected` dependency.
- Expanded README coverage for the current API surface, Linux runtime
  dependencies, benchmark entrypoints, Aeron comparison limits, and known gaps.
- Localized and clarified API, core, benchmark, test, and platform comments so
  ownership, synchronization, and failure contracts are easier to audit.
- Updated `salias_bench_compare` to use batch polling with an explicit
  `--poll-limit` option for fairer comparison with Aeron's fragment limit.
- Updated the Aeron comparison runner to use forked producer/subscriber
  endpoints for both salias named IPC and Aeron IPC.

### Removed

- Removed the now-redundant channel implementations (`broadcast`, `bulk`, `mpsc`,
  `spsc`, `shared_mpmc`, `shared_mpsc`, `shared_spsc`, `channel_config`), the L6
  metrics module (`counters` / `layout` / `reader` / `error`), the platform futex
  primitive, and the `futex_wait` / `busy_spin` / `yielding` wait strategies,
  consolidating the wait layer onto `SpinPause` plus the hybrid engine.
- Removed the obsolete `channel_compare` and `smoke` benchmarks and the
  broadcast / bulk / mpsc / spsc / metrics / futex / wait-strategy unit tests,
  superseded by the hybrid channel and latency-histogram tests.

### Fixed

- Fixed ring capacity checks after position wrap so recreated publisher state
  cannot bypass backpressure through unsigned underflow.
