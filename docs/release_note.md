# Release Notes

本文件按时间顺序记录每次发版，每个版本节与同名的 git tag 对齐。tag 注解内容以此处对应版本节为唯一真源。

## v1.2.0 - 2026-07-11

- **版本级别**：次版本（minor）
- **Git 提交消息**：chore: 发版 v1.2.0
- **git tag**：v1.2.0
- **自上次 tag（v1.1.0）以来的变更摘要**：
  - **新增**：独立进程的 salias / Aeron 基准 worker（Publisher / Subscriber），由外部协调而非 fork；最终矩阵覆盖 2P1S/2P2S、1/4/64 MiB 容量、FIFO 公平、Ordered batch 1/8、消息校验与背压统计。
  - **新增**：独立交互式 HTML 动画，直观说明 Channel、Publisher/Subscriber、Producer/Consumer、per-producer MagicRing、顺序、fanout、release 与背压的消息流。
  - **变更**：用一份独立进程最终性能报告替代历史对比文档与日志，包含机器配置、480 份校验过的原始样本、生成的摘要，以及内嵌在 README 的核心 salias/Aeron 对照表。
  - **变更**：更新优化复盘，将独立进程最终报告作为当前唯一的性能真相来源。
  - **杂项**：将 `*.log` 加入 `.gitignore`，基准与运行时日志不再纳入版本库。
  - **移除**：移除被替代的 Tencent/Aeron 对比报告与采用旧测试框架或冲突测量条件的中间基准日志。

## v1.3.0 - 2026-07-12

- **版本级别**：次版本（minor）
- **Git 提交消息**：feat: 新增 CMake 安装导出能力并适配 macOS 共享内存命名
- **git tag**：v1.3.0
- **自上次 tag（v1.2.0）以来的变更摘要**：
  - **新增**：CMake 安装导出能力——顶层 `install` 规则导出 `saliasTargets`（`salias::` 命名空间）并安装 salias 与 core 公共头，通过 `CMakePackageConfigHelpers` 生成 `saliasConfig.cmake` / `saliasConfigVersion.cmake`（`SameMajorVersion` 兼容）；下游可用 `find_package(salias CONFIG REQUIRED)` + `target_link_libraries(... salias::salias)` 直接消费。`salias_core` / `salias` 补 `salias::` ALIAS 目标，`salias` 补 `INSTALL_INTERFACE` 头路径。
  - **新增**：安装消费冒烟测试 `salias_install_consumer`（`cmake/saliasConfig.cmake.in` + `cmake/install_consumer_test.cmake` + `test/install/` 最小下游工程）。
  - **新增**：最长通道名（128 字符）创建 / 连接 / 收发测试，以及 macOS 专属的显式 HugePage 不可用测试。
  - **变更**：控制段与 ring 的 POSIX shm 名称改为对公共通道名做 FNV-1a 64 位哈希编码后的固定 16 位 hex 键（`/salias-<16hex>-c`、`/salias-<16hex>-r-<id>`），Linux 与 macOS 共用同一资源寻址，解决 macOS shm 名称长度限制导致 `shm_open` 失败的问题；公共 API 不变。
  - **修复**：非 Linux 平台（macOS）显式 `HugePage` 请求在创建期即返回 `PlatformFail`，不再触碰 Linux 专属 hugetlbfs 路径。
  - **文档**：README 由 "Linux-only" 改为 "支持 Linux 与 macOS"，补充 CMake 安装与 `find_package` 消费说明；新增 macOS 安装支持设计与实现计划。

## v2.0.0 - 2026-07-12

- **版本级别**：主版本（major）
- **Git 提交消息**：fix: 对齐公共 API 契约与错误边界
- **git tag**：v2.0.0
- **自上次 tag（v1.3.0）以来的变更摘要**：
  - **破坏性变更**：移除未实现但曾公开暴露的 `Config::fixed_size` 与 `Config::record_size`，明确当前公共协议只支持变长帧；控制段使用保留槽位维持内部布局尺寸。
  - **错误边界**：新增 `Error::OutOfMemory`，`Channel::create()` 与 `Channel::connect()` 捕获 `std::bad_alloc` 并通过 `std::expected` 返回 typed error；OOM 回滚路径清理已创建的控制段与 ring 名称。
  - **异常契约**：移除 `Channel::publisher()` 与 `Channel::subscriber()` 的错误 `noexcept`，端点状态分配失败不再导致 `std::terminate`。
  - **配置契约**：公共注释和 L7 文档明确 ring 容量必须满足 2 的幂、系统页对齐，以及显式大页大小对齐要求。
  - **版本治理**：CMake 与 vcpkg 包版本统一为 `2.0.0`，新增 CMake/vcpkg/精确 Git tag 一致性测试，修复安装包版本仍为 `0.0.1` 的问题。
  - **测试与文档**：新增配置表面、容量约束、分配失败契约测试，以及 API 契约对齐设计和实施计划。
