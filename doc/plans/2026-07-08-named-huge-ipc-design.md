# Named Huge Page IPC And Page Benchmark Design

## Goal

Support explicit huge pages for public named cross-process channels, then compare
normal pages, 2 MiB huge pages, and 1 GiB huge pages for MPSC and MPMC IPC
workloads before comparing the same workloads against Aeron IPC.

## Scope

- Support `Config::huge` on named SPSC/MPSC/MPMC ring data objects.
- Keep the named control block as POSIX shm.
- Use a hugetlbfs file for the named ring data object when huge pages are
  requested.
- Extend the salias IPC benchmark tool with a page-size selector.
- Extend the Aeron comparison script to run salias page-size variants and Aeron
  IPC for MPSC and MPMC.

## Non-Goals

- Do not add automatic fallback from huge pages to normal pages.
- Do not implement NUMA binding.
- Do not try to force Aeron onto hugetlb pages in this change. Aeron remains the
  default IPC baseline.
- Do not claim benchmark winners unless the benchmark script is run on the target
  environment and the raw results are recorded.

## Architecture

Named channels continue to use `/salias-<name>-ctl` POSIX shm for metadata and
positions. The ring data backing is selected by the owner at create time:

- `HugePage::None`: existing POSIX shm object `/salias-<name>-ring`.
- `HugePage::Size2MB` or `HugePage::Size1GB`: hugetlbfs regular file named
  `salias-<name>-ring` under a configurable mount directory.

The control block stores the selected ring backend and huge page size in its
existing `flags` field. `Channel::connect(name)` reads those flags and opens the
same backend as the owner. The ring file is mapped through
`Mapping::map_shared_fd(fd, MapOptions{.size = capacity, .huge = huge})` so the
double-mapped magic ring path is shared by normal and huge backends.

The default hugetlbfs mount path is `/dev/hugepages`. For benchmark and test
flexibility, an environment variable `SALIAS_HUGETLBFS_DIR` can override the
directory. Missing mount, permission failures, or hugetlb pool exhaustion return
`Error::PlatformFail`.

## Control Flags

Use the existing `NamedControl::flags` field:

- bit 0: ring backend is hugetlbfs instead of POSIX shm.
- bits 1-2: huge page size code.
  - `00`: none
  - `01`: 2 MiB
  - `10`: 1 GiB

Connections reject unknown flag combinations as `BadConfig`.

## Lifecycle

For normal pages, lifecycle remains unchanged: owner creates and later unlinks the
control and ring shm names.

For huge pages, owner creates the hugetlbfs ring file with `O_CREAT | O_EXCL`,
truncates it to the requested capacity, maps it, then unlinks it in the named
state destructor. Existing mappings stay valid after unlink, matching POSIX shm
lifetime behavior.

If create fails after one resource is created, cleanup removes both the control
shm object and whichever ring backend was created.

## Benchmarks

`salias_ipc_compare` gains:

```text
--page normal|huge2m|huge1g
```

Result rows include `page=<page>`. A huge page request that cannot be created
returns a non-zero status from the tool; the wrapper script converts that into a
machine-readable `SKIP` row.

`run_release_compare.sh` runs:

- salias MPSC 4P/1C for `normal`, `huge2m`, `huge1g`
- salias MPMC 4P/2C for `normal`, `huge2m`, `huge1g`
- Aeron IPC MPSC 4P/1C
- Aeron IPC MPMC 4P/2C

The script preserves warmup and measured round controls through the existing
environment variables.

## Tests

Deterministic tests:

- named huge with capacity not aligned to the selected huge page size returns
  `BadConfig`.
- named huge with a missing hugetlbfs directory returns `PlatformFail` and leaves
  no POSIX shm control object.
- `salias_ipc_compare --page normal` prints `page=normal`.

Conditional tests:

- if `/dev/hugepages` or `SALIAS_HUGETLBFS_DIR` is usable and huge pages are
  reserved, named huge MPSC/MPMC can be exercised by the benchmark manually.

