# MPSC and MPMC Example Design

Goal: add runnable publisher/subscriber examples for salias named IPC MPSC and MPMC channels.

Design:
- Add `example/` with four binaries: `salias_mpsc_subscriber`, `salias_mpsc_publisher`, `salias_mpmc_subscriber`, and `salias_mpmc_publisher`.
- Keep subscribers as channel owners for the common demo path. `mpmc_subscriber --create` owns the channel; `mpmc_subscriber` without `--create` connects as an additional fanout subscriber.
- Publishers connect to an existing named channel and publish text payloads with `try_claim/commit`.
- Add a CTest smoke script that launches subscribers, waits for their ready output, runs publishers, and verifies all processes exit successfully.

Verification:
- Configure and build with `SALIAS_BUILD_EXAMPLES=ON`.
- Run `ctest -R salias_example_smoke` in the build tree.
