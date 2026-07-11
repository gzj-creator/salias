# macOS 与安装支持设计

## 目标

让 salias 的普通 POSIX 共享内存模式在 Linux 与 macOS 上使用同一套公共 API，支持长 channel 名称、跨进程发布订阅和标准 CMake 安装；Linux 专属 HugePage/NUMA 能力在 macOS 上明确报告不可用。

## 现状

- magic ring 已在 Linux 使用 `memfd_create`，在 macOS 使用立即 unlink 的临时文件，并通过两次 `MAP_FIXED | MAP_SHARED` 建立双映射。
- 具名 channel 使用 `shm_open`，内部名称直接拼接公共 channel 名称。macOS 的 POSIX shm 名称限制较短，现有示例生成的名称会导致 `shm_open` 返回失败。
- 工程可以直接构建静态库，但没有 `install(...)`、导出 target 或 package config。
- HugePage 具名 channel 依赖 Linux hugetlbfs；NUMA 当前没有跨平台实现。

## 方案

### POSIX shm 名称

公共 channel 名称继续允许当前字符集和最大 128 字符，不改变 API。所有控制段和 ring 的 OS 资源名称统一使用确定性的 64-bit FNV-1a 哈希编码：

- 控制段：`/salias-<16 hex>-c`
- producer ring：`/salias-<16 hex>-r-<producer id>`

哈希输入包含完整公共名称；控制段和 ring 使用不同后缀区分。名称长度固定且远低于 macOS 限制，同一公共名称在独立进程中可重建相同资源名。哈希仅用于资源寻址，不承担安全或身份认证职责。

### 平台能力

- `HugePage::None` 在 Linux/macOS 使用 POSIX shm 和 magic-ring 双映射。
- Linux 保留现有 hugetlbfs HugePage 行为。
- macOS 对显式 HugePage 请求返回 `Error::PlatformFail`，不访问 Linux 默认 hugetlbfs 路径。
- NUMA 保持平台层 `NumaUnavailable` 行为，不增加虚假的 macOS 实现。

### 安装与下游使用

- 安装 `src/salias/include/salias` 下的公共头文件。
- 安装 `salias` 和内部链接所需的 `salias_core` 静态库。
- 导出命名空间 target：`salias::salias`、`salias::salias_core`。
- 生成并安装 `saliasConfig.cmake` 与 `saliasConfigVersion.cmake`。
- 下游通过 `find_package(salias CONFIG REQUIRED)` 和 `target_link_libraries(app PRIVATE salias::salias)` 使用。

## 错误处理

- 公共名称格式错误继续返回 `BadConfig`。
- POSIX shm 创建、打开或映射失败继续返回 `PlatformFail`/`NotFound`，保持现有 API。
- macOS 显式 HugePage 请求在创建控制段前失败，避免遗留半成品资源。

## 测试

- API 回归测试：128 字符公共名称可以创建、连接和收发。
- API 平台测试：macOS 显式 HugePage 请求失败且不创建资源。
- 现有 example smoke 使用原长名称并在 macOS 通过。
- 安装到临时 prefix 后，配置一个独立下游 CMake 工程，使用 `find_package` 编译并链接。
- Linux 保持现有构建和测试行为。

