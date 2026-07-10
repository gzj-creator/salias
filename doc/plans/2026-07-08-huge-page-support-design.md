# Huge Page Support Design

## Goal

Make `MapOptions::huge` a real explicit huge page request for anonymous in-process
ring mappings, and expose the same choice on public `salias::Config`.

## Scope

- Implement explicit hugetlb-backed `memfd_create` for `Mapping::create()`.
- Support `HugePage::Size2MB` and `HugePage::Size1GB` when the host kernel and
  huge page pool allow it.
- Add public `salias::HugePage` and `Config::huge`.
- Pass public huge page settings into in-process core channels.
- Named hugetlbfs-backed IPC is handled by the follow-up
  `2026-07-08-named-huge-ipc-design.md` design.

## Non-Goals

- Do not silently downgrade a huge page request to normal pages.
- Do not implement NUMA binding.
- Do not add benchmark claims without a dedicated benchmark run.

## Architecture

`Mapping::create()` remains the only creator for anonymous ring backing storage.
When `MapOptions::huge` is `None`, it keeps the current normal `memfd` path. When
huge pages are requested, it creates the backing fd with `MFD_HUGETLB` plus the
requested huge page size flag.

The backing fd already determines page backing, so the two aliasing `mmap()` calls
do not need `MAP_HUGETLB`. The mapping code still reserves a contiguous virtual
range first, then maps the same fd twice with `MAP_FIXED`.

`Mapping::map_shared_fd()` maps an already-created fd. It does not create huge
page backing itself. It should continue to validate the size and NUMA options,
but huge page creation semantics belong to the fd creator.

## Validation

Normal mappings keep the existing constraints: size must be non-zero, a power of
two, aligned to the system page size, and within `size * 2` overflow limits.

Huge mappings add an explicit alignment rule:

- `Size2MB`: size must be a multiple of 2 MiB.
- `Size1GB`: size must be a multiple of 1 GiB.

Invalid size returns `PlatformError::InvalidSize`. Failure to create or size a
hugetlb-backed fd returns `PlatformError::HugePageUnavailable`.

## Public API

Add this to `salias::Config`:

```cpp
enum class HugePage { None, Size2MB, Size1GB };

struct Config {
  std::string name;
  Mode mode = Mode::Spsc;
  std::size_t capacity = 1u << 20;
  HugePage huge = HugePage::None;
  bool fixed_size = false;
  std::size_t record_size = 0;
  WaitKind wait = WaitKind::SpinPause;
};
```

In-process channels pass the setting into `channel::ChannelConfig`. Named
channels use the follow-up hugetlbfs ring backend design rather than sharing the
anonymous `memfd` creation path.

## Testing

Deterministic tests should not require a machine with configured huge pages:

- `Mapping::create(Size2MB, size=page_size)` returns `InvalidSize`.
- `Mapping::create(Size2MB, size=2MiB)` either succeeds and proves aliasing, or
  returns `HugePageUnavailable`.
- public facade capacity validation rejects huge page requests whose capacity is
  not aligned to the requested huge page size.

The successful huge mapping test is conditional because CI machines often do not
reserve hugetlb pages.
