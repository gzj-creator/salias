# salias API Contract Alignment Design

## Goal

Align the shipped package metadata and public API contract for the salias v2.0.0 release.

## Version Metadata

- Set the CMake project version and vcpkg package version to `2.0.0`.
- Add a repository check that compares the configured project version with the exact Git tag when the current commit is tagged.
- Keep untagged development builds valid; only reject a mismatch when an exact semantic-version release tag exists.

## Fixed-Size Configuration

- Remove `Config::fixed_size` and `Config::record_size` because no fixed-size protocol is implemented.
- Remove the unused named-control `record_size` field while preserving the control block size and offsets with reserved padding.
- Remove validation branches that only reject those fields.

## Capacity Contract

- Document that capacity must be a power of two and a multiple of the host system page size.
- Document that explicit huge-page configurations additionally require capacity to be a multiple of the selected huge-page size.
- Preserve the existing runtime validation and typed `BadConfig` behavior.

## Allocation Failure Contract

- Add `Error::OutOfMemory` to the public error enum.
- Catch `std::bad_alloc` at `Channel::create()` and `Channel::connect()` boundaries and return `OutOfMemory`.
- Remove `noexcept` from `publisher()` and `subscriber()` because they allocate shared endpoint state and retain their existing return types.
- Document that endpoint allocation may throw while channel factory allocation failures are typed results.

## Verification

- Add regression tests for removed configuration surface, package-version consistency, capacity rejection, and allocation-error typing where deterministic injection is practical.
- Build the project with tests enabled.
- Run CTest, API tests, and install-consumer validation.
