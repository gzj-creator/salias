# Channel API 仓库内使用矩阵

日期：2026-10-30（Asia/Singapore）

本文记录仓库内对 Channel 产品入口及外围能力的引用。它是 Phase 0 的审计基线，只覆盖
salias 自身的代码、测试、工具、安装包和 module；仓库外消费者不在本次接口收敛的审计范围内。

## 1. 审计方法与边界

审计范围包括 `test/`、`bench/`、`example/`、`test/install/`、`test/mcpp/`、CMake
安装规则、`src/salias.cppm`、`src/salias_extras.cppm` 和 `mcpp.toml`。检索命令使用符号名全量搜索，而不是只看
README 或单个示例。当前工作区没有 `AGENTS.md`；本文不把仓库内引用外推为下游使用。

## 2. 使用矩阵

| 能力与当前入口 | 测试 | benchmark | 示例 | 安装消费 | mcpp/module | 当前处置 |
|---|---|---|---|---|---|---|
| 规范通用流 `Channel<Mode[, Schema]>` | `test/api/*`、`test/channel/*`、`test/mcpp/*` 的规范路径 | `bench/hotpath_ab.cpp`、`bench/receiver_scan.cpp`、`bench/channel_stress.cpp`、`bench/round2*.cpp` 等 | `example/minimal_*`、`example/mpsc_*` | 通过 `salias::salias` 安装 | `salias.hpp` 与 `import salias;` 导出 | **core**；作为唯一通用主入口 |
| 四个 raw 便捷别名 `FifoMpscChannel`、`FifoMpmcChannel`、`OrderedMpscChannel`、`OrderedMpmcChannel` | 仅 `test/api/channel_alias_compat_test.cpp` 保留同型与弃用窗口回归 | 无；已迁移到 `Channel<Mode>` | 无；已迁移到 `Channel<Mode>` | 未使用 | `src/salias.cppm` 仍导出；`test/mcpp` 使用规范入口 | **compat**；4.1 编译期弃用，最早 5.0 删除 |
| 显式 `Schema<T, Id, Version>` 辅助模板 | `test/api/schema_helper_test.cpp`；typed latest/Channel 既有测试仍覆盖手写 schema | 通过 typed Channel 参数间接使用 | 暂无 | 经聚合头可见 | `test/mcpp/typed_schema.hpp`、`import.cpp`、`src/salias.cppm` | **core**；无运行时成本，版本保持显式 |
| 固定快速流 `FixedChannel`、`FixedPublisher`、`FixedSubscriber`、`FixedPublishClaim` | `test/api/fixed_channel_test.cpp` 及固定 Ring 测试 | `bench/fixed_channel_poll_compare.cpp` | 暂无 | `test/install/main.cpp` 创建并收发 | `salias.hpp` 与 `import salias;` 导出 | **core**；独立数据面，不与通用 Channel 合并 |
| 规范 latest fanout `LatestMpmcChannel<Capacity, Watermark[, Schema]>` | `test/api/latest_subscription_test.cpp`、`channel_api_test.cpp`、`priority_channel_test.cpp`、`allocation_contract_test.cpp` | `bench/channel_stress.cpp`、`bench/round2.cpp`、`bench/round2_probe.cpp` | 暂无 | `test/install/main.cpp` 创建并收发 typed latest | `test/mcpp/import.cpp`、`test/mcpp/header_probe.cpp`、`src/salias.cppm` | **core**；raw/typed 共用的唯一最新窗口入口 |
| 直接 `Channel<Mode::LatestMpmc>` | `test/api/channel_api_test.cpp` 用编译期 concept 断言工厂不可调用 | 已迁移到 `LatestMpmcChannel` | 已迁移到 `LatestMpmcChannel` | 未使用 | `header_probe.cpp` 通过 wrapper 分派 | **removed**；不提供直接 `create()`/`connect()` |
| `salias::extras::LatestByKeyChannel` 及消息/端点 | `test/api/latest_by_key_test.cpp`、`allocation_contract_test.cpp` | `bench/latest_by_key.cpp`、`bench/channel_stress.cpp` | 暂无 | `test/install/extras/main.cpp` 跨进程收发 | `<salias/extras/extras.hpp>`、`import salias.extras;`、`salias::extras` | **extras**；实现位于独立 `salias_extras` |
| `salias::extras::Priority*` | `test/api/priority_channel_test.cpp` | `bench/round2.cpp`、`bench/round2_probe.cpp`、`bench/channel_stress.cpp` | 暂无 | `test/install/extras/main.cpp` 编译并验证失败语义 | `<salias/extras/extras.hpp>`、`import salias.extras;`、`salias::extras` | **extras**；不再进入核心安装消费者 |
| `salias::extras::Ringbuffer` | `test/ring/ringbuffer_test.cpp` | 暂无 | 暂无 | `test/install/extras/main.cpp` 验证丢旧 | `<salias/extras/extras.hpp>`、`import salias.extras;`、`salias::extras` | **extras**；header-only，随 extras include 根发布 |
| 旧顶层 extras 名称与三个旧 include | 仅 `test/api/extras_compat_test.cpp` 验证同型与弃用窗口 | 无；已迁移 | 无；已迁移 | 无；extras 安装消费者使用规范名称 | 核心 umbrella/module 不再导出；旧转发头随 extras component | **compat**；本版本不删除 |

## 3. 关键缺口

- 核心安装消费者与 mcpp 核心 module 只使用核心路径；extras 安装消费者链接
  `salias::extras`，mcpp import 单元显式 `import salias.extras;`。
- 旧 extras 名称只由 `test/api/extras_compat_test.cpp` 和三个转发头保留；主测试、benchmark、
  install consumer 和 mcpp consumer 均使用 `salias::extras::*`。
- `LatestMpmcChannel` 同时支持 raw 和 typed payload；第三个模板参数省略时是 raw，显式传入时
  保留原有 schema ID、版本与 payload 大小校验。直接 `Channel<Mode::LatestMpmc>` 工厂已关闭。
- 仓库外使用证据没有采集，也不作为本次接口收敛的门禁；extras 只迁移边界，不删除旧名称。

## 4. 证据命令

```bash
rg -n 'FifoMpscChannel|FifoMpmcChannel|OrderedMpscChannel|OrderedMpmcChannel' \
  test bench example test/install test/mcpp
rg -n 'LatestMpmcChannel|Channel<.*LatestMpmc' test bench example test/install test/mcpp
rg -n 'LatestByKeyChannel|LatestByKeyPublisher|LatestByKeySubscriber|LatestByKeyMessage' \
  test bench example test/install test/mcpp
rg -n 'PriorityPublisher|PrioritySubscriber|PriorityMessage|PriorityWaitPolicy' \
  test bench example test/install test/mcpp
rg -n 'Ringbuffer' test bench example test/install test/mcpp
rg -n 'using ::salias::' src/salias.cppm
rg -n 'using ::salias::extras::' src/salias_extras.cppm
rg -n 'salias::extras|salias/extras/' test bench example test/install test/mcpp
```
