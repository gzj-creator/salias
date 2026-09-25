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

### Changed

- 对齐 salias/Aeron 最终基准的 64B payload、`try_claim`/`tryClaim` 零拷贝发布与空转/背压 `yield` 策略，新增可选 `--aligned` 模式，减少测试策略差异对 MPSC/扇出结论的干扰。
- 优化进程内与跨进程 MPSC 热路径：用连续已提交水位统一发布帧头与 payload，为乱序 commit 保留空洞保护，并缓存 ring 几何信息以减少原子操作、取模和重复共享状态读取。
- 扩展 FIFO/Ordered 批量接收与轮询逻辑，在三生产者、稀疏 ring、跨 ring 空洞、部分批次和 fanout 背压下保持顺序、公平性与 payload 生命期。
- 新增 `salias_hotpath_ab`、`salias_receiver_scan` 及 ABBA 对照脚本，并保留 2026-09-25 的热路径、扫描、多生产者与被拒绝方案原始结果。

### Tests

- 新增 commit 可见性契约测试，覆盖同一 producer 连续 claim 后乱序 commit，验证 consumer 不会越过未提交空洞且共享可见位置不会回退。
- 新增 FIFO 三 ring 公平扫描、Ordered 跨 ring 批量、fanout 慢消费者保留、并发回绕背压与 SPSC 本地游标回归测试。

### Docs

- 更新 L7 API、优化复盘与低延迟窗口建议，新增 2026-09-25 性能优化验证报告和可复核的基准原始记录。

## [v2.0.0] - 2026-07-12

### Changed

- 移除未实现但曾公开暴露的 `Config::fixed_size` 与 `Config::record_size`，明确当前公共协议只支持变长帧；控制段以保留槽位维持内部布局尺寸。
- `Channel::publisher()` 与 `Channel::subscriber()` 不再错误声明为 `noexcept`，端点状态分配失败可正常传播 `std::bad_alloc`，避免触发 `std::terminate`。
- 公共配置与 L7 API 文档明确 ring 容量必须同时满足 2 的幂、系统页对齐，以及显式大页大小对齐要求。

### Fixed

- 新增 `Error::OutOfMemory`，`Channel::create()` 与 `Channel::connect()` 捕获动态内存分配失败并通过 `std::expected` 返回 typed error。
- 创建通道过程中发生分配失败时回收已创建的控制段与 ring 名称，避免 OOM 路径遗留共享内存资源。
- CMake 与 vcpkg 包版本统一为 `2.0.0`，新增 CMake/vcpkg/精确 Git tag 一致性检查，修复安装包版本长期停留在 `0.0.1` 的问题。

### Tests

- 新增不依赖 GTest 的配置表面、容量约束和分配失败契约测试，并保留安装消费者测试覆盖。

### Docs

- 更新 L7 公共 API 文档，并新增 API 契约对齐设计与实施计划。

## [v1.3.0] - 2026-07-12

### Added

- CMake 安装导出能力：顶层新增 `install` 规则导出 `saliasTargets`（`salias::`
  命名空间）、安装 salias 与 core 公共头，并通过 `CMakePackageConfigHelpers`
  生成 `saliasConfig.cmake` / `saliasConfigVersion.cmake`（`SameMajorVersion`
  兼容）。下游工程可通过 `find_package(salias CONFIG REQUIRED)` +
  `target_link_libraries(... salias::salias)` 直接消费。`salias_core` 与
  `salias` 补 `salias::` ALIAS 目标，`salias` 增加 `INSTALL_INTERFACE` 头路径。
- 安装消费冒烟测试：新增 `cmake/saliasConfig.cmake.in` 包配置模板、
  `cmake/install_consumer_test.cmake` 安装并配置/构建下游工程的脚本，以及
  `test/install/` 最小 `find_package` 消费示例，统一挂到 CTest
  `salias_install_consumer`。
- 测试新增 `LongChannelNameSupportsCreateAndConnect`（最长 128 字符通道名创建 /
  连接 / 收发）与 macOS 专属 `ExplicitHugePagesAreUnavailableOnMacOS`。

### Changed

- `channel.cpp` 对公共通道名引入 FNV-1a 64 位哈希编码（`channel_name_hash` +
  `encoded_channel_name`），控制段与 ring 的 POSIX shm 名称统一改用固定 16 位
  hex 键（`/salias-<16hex>-c`、`/salias-<16hex>-r-<id>`），Linux 与 macOS 共用
  同一资源寻址，规避 macOS 对 POSIX shm 名称的短限制。公共 API、字符集与最大长度
  不变。
- README 由 "Linux-only" 改为 "支持 Linux 与 macOS"，补充 CMake 安装与
  `find_package` 消费说明。

### Fixed

- `create_named_state` 在非 Linux 平台（macOS）对显式 `HugePage` 请求提前返回
  `PlatformFail`，不再依赖后端隐式失败或触碰 Linux 专属 hugetlbfs 路径。

### Docs

- 新增 `docs/plans/2026-07-11-macos-install-support-design.md` 与
  `2026-07-11-macos-install-support.md` 设计与实现计划。

## [v1.2.0] - 2026-07-11

### Added

- Added independent-process salias and Aeron benchmark workers for Publisher and
  Subscriber roles, coordinated externally without forking benchmark workers.
  The final matrix covers 2P1S/2P2S, 1/4/64 MiB capacities, FIFO fairness,
  Ordered batch 1/8, message validation, and backpressure statistics.
- Added a standalone interactive HTML animation explaining Channel,
  Publisher/Subscriber, Producer/Consumer, per-producer MagicRing, ordering,
  fanout, release, and backpressure message flow.

### Changed

- Replaced historical performance comparison documents and logs with one final
  independent-process report, machine configuration, 480 validated raw samples,
  generated summaries, and the core salias/Aeron table embedded in README.
- Updated the optimization retrospective to treat the final independent-process
  report as the sole current performance truth source.

### Chore

- Added `*.log` to `.gitignore` so benchmark and runtime logs are no longer
  tracked by version control.

### Removed

- Removed superseded Tencent/Aeron comparison reports and intermediate benchmark
  logs that used earlier harnesses or conflicting measurement conditions.

## [v1.1.0] - 2026-07-11

### Added

- macOS platform support: the magic-ring double-mapping backend now builds and
  runs on macOS alongside Linux. macOS backs the ring with an immediately-unlinked
  `mkstemp` temp file where Linux uses `memfd_create`; `MAP_POPULATE` is applied
  only where the platform defines it (via a `kPopulateFlag` fallback in
  `mapping.cpp` and `channel.cpp`), and Linux-only headers (`linux/memfd.h`,
  `sys/syscall.h`) are guarded by `__linux__`. Explicit huge-page backends remain
  Linux-only (rejected on macOS).
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
- Two minimal end-to-end demos (`example/minimal_fifo.cpp`,
  `example/minimal_ordered.cpp`) plus `example/README.md` showing the shortest
  path to FIFO vs Ordered MPSC semantics; wired into the build and test graph
  via `example/CMakeLists.txt`.

### Changed

- Relaxed the top-level build from Linux-only to Linux + macOS; added a
  `BUILD_TESTING` option, defaulted `SALIAS_BUILD_BENCHMARKS` to OFF, and made
  Google Benchmark / GTest optional (`find_package ... QUIET`) with graceful
  skip messages. `salias_ipc_compare` is now gated to Linux-only.
- Refactored `SharedHybridMpscChannel` sequence-number acquisition from an
  immediately-invoked lambda to a plain if/else on both the single-frame and
  batch claim paths (portability cleanup; behavior unchanged).
- Named IPC control block now carries `publication_window`; `kNamedVersion`
  raised `3 -> 4` (binary-incompatible protocol bump).

### Docs

- Added `doc/13-optimization-retrospective.md`: a full retrospective of the
  optimization timeline (per-producer architecture → shared-state dedup →
  consumer hot path → frame-header micro-opts) with final recommended configs
  and measurements; referenced from `README.md` and the Tencent comparison doc.
- Appended the final 64-bit frame-header publish optimization re-verification to
  the Tencent 4 vCPU Aeron comparison (FIFO batch=1 lifted to 115.0% of Aeron;
  Ordered batch=8 at 93.4%), with the matching batch1/8/16 × 20-round raw logs.
- Updated the Tencent 4 vCPU Aeron comparison with flow-control-window and
  Phase A re-verification results (FIFO throughput lifted to ~85% of Aeron; a
  128 KiB window cuts FIFO p50 from 5.37 ms to 147 µs), plus the matching raw
  benchmark logs.
- Added Chinese Doxygen header docs and inline rationale comments across the
  entire C++ tree (src/test/bench/tools/example): file-level
  `@file/@brief/@details` with layering and thread/process model, per-function
  `@param/@return/@note`, and inline notes on memory ordering, flow control,
  sequence wraparound, and cache-line layout. Code unchanged (verified by a
  comment-stripped skeleton diff against the pre-change baseline).

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
