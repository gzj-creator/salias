# salias API Contract Alignment Implementation Plan

1. Add a failing metadata check for CMake, vcpkg, and exact Git tag consistency.
2. Update package versions to `2.0.0` and wire the check into CTest.
3. Add compile-time API assertions proving fixed-size fields are absent.
4. Remove fixed-size fields and preserve named-control layout with reserved storage.
5. Add capacity contract tests for system-page and huge-page alignment.
6. Update public comments to describe all capacity constraints.
7. Add `Error::OutOfMemory` and deterministic allocation-failure coverage.
8. Catch allocation failures in channel factory boundaries.
9. Remove incorrect endpoint `noexcept` declarations and update API documentation.
10. Configure a fresh test build and run focused then complete validation.
