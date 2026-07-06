# Changelog

## Maintenance Notes

This file records user-facing and engineering-significant changes before they
are released.

- Keep pending local commits under `## [Unreleased]`.
- When creating a release tag, move relevant entries into a version section
  formatted as `## [vX.Y.Z.W] - YYYY-MM-DD`.
- Group changes by `Added`, `Changed`, `Fixed`, `Docs`, or similar sections.
- Prefer concise behavioral summaries over mechanical file lists.

## [Unreleased]

### Added

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

### Fixed

- Fixed ring capacity checks after position wrap so recreated publisher state
  cannot bypass backpressure through unsigned underflow.
