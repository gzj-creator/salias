# Channel API 仓库内使用矩阵

日期：2026-10-30（Asia/Singapore）

本文记录仓库内对 Channel 产品入口及外围能力的引用。它是 Phase 0 的审计基线，只覆盖
salias 自身的代码、测试、工具、安装包和 module；仓库外消费者不在本次接口收敛的审计范围内。

## 1. 审计方法与边界

审计范围包括 `test/`、`bench/`、`example/`、`test/install/`、`test/mcpp/`、CMake
安装规则、`src/salias.cppm` 和 `mcpp.toml`。检索命令使用符号名全量搜索，而不是只看
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
| `LatestByKeyChannel` 及消息/端点 | `test/api/latest_by_key_test.cpp`、`allocation_contract_test.cpp` | `bench/latest_by_key.cpp`、`bench/channel_stress.cpp` | 暂无 | 未使用 | `header_probe.cpp`、`import.cpp`、`src/salias.cppm` | **extras 候选**；外部使用未确认，不删除 |
| `PriorityPublisher`、`PrioritySubscriber`、`PriorityMessage`、`PriorityWaitPolicy` | `test/api/priority_channel_test.cpp` | `bench/round2.cpp`、`bench/round2_probe.cpp`、`bench/channel_stress.cpp` | 暂无 | `test/install/main.cpp` 直接依赖 | `header_probe.cpp`、`import.cpp`、`src/salias.cppm` | **extras 候选**；安装消费仍耦合，Phase 3 处理 |
| `Ringbuffer` | `test/ring/ringbuffer_test.cpp` | 暂无 | 暂无 | 未使用 | `salias.hpp` 与 `import salias;` 导出 | **extras 候选**；外部使用未确认，不删除 |

## 3. 关键缺口

- 安装消费 `test/install/main.cpp` 当前直接使用 `PrioritySubscriber` 和
  `PriorityWaitPolicy`，证明核心安装归档尚未与 extras 候选解耦；该问题留给 Phase 3。
- mcpp 的 header probe 和 import test 同时覆盖规范核心路径、旧别名、Latest、LatestByKey
  和 Priority。Phase 1 只迁移旧别名并保留独立兼容测试，不在本阶段拆分 mcpp extras。
- `LatestMpmcChannel` 同时支持 raw 和 typed payload；第三个模板参数省略时是 raw，显式传入时
  保留原有 schema ID、版本与 payload 大小校验。直接 `Channel<Mode::LatestMpmc>` 工厂已关闭。
- 仓库外使用证据没有采集，也不作为本次接口收敛的门禁；`LatestByKey`、Priority 和 Ringbuffer
  保持当前导出，Phase 3 前不删除、不移动。

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
```
