# Channel API 仓库内使用矩阵

日期：2026-10-30（Asia/Singapore）

本文记录收敛前仓库内对 Channel 产品入口及外围能力的引用。它是 Phase 0 的审计基线，
不是下游使用证明；仓库外消费者见
[外部证据审计](channel-api-downstream-evidence.md)。

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
| 规范 latest fanout `LatestMpmcChannel` | `test/api/latest_subscription_test.cpp`、`channel_api_test.cpp`、`priority_channel_test.cpp`、`allocation_contract_test.cpp` | `bench/channel_stress.cpp`、`bench/round2.cpp`、`bench/round2_probe.cpp` | 暂无 | 由 CMake 聚合头间接可见 | `test/mcpp/import.cpp`、`src/salias.cppm` | **core**；唯一推荐 latest wrapper |
| 直接 `Channel<Mode::LatestMpmc>` | `test/api/channel_api_test.cpp` 的 typed latest/非法配置测试段、`allocation_contract_test.cpp` | 通用 `Channel<Mode>` 模板实例化路径 | 暂无 | 未直接实例化 | `header_probe.cpp` 的五模式分派包含该实例化 | **compat**；停止主推，typed latest 迁移待 Phase 2 |
| `LatestByKeyChannel` 及消息/端点 | `test/api/latest_by_key_test.cpp`、`allocation_contract_test.cpp` | `bench/latest_by_key.cpp`、`bench/channel_stress.cpp` | 暂无 | 未使用 | `header_probe.cpp`、`import.cpp`、`src/salias.cppm` | **extras 候选**；外部使用未确认，不删除 |
| `PriorityPublisher`、`PrioritySubscriber`、`PriorityMessage`、`PriorityWaitPolicy` | `test/api/priority_channel_test.cpp` | `bench/round2.cpp`、`bench/round2_probe.cpp`、`bench/channel_stress.cpp` | 暂无 | `test/install/main.cpp` 直接依赖 | `header_probe.cpp`、`import.cpp`、`src/salias.cppm` | **extras 候选**；安装消费仍耦合，Phase 3 处理 |
| `Ringbuffer` | `test/ring/ringbuffer_test.cpp` | 暂无 | 暂无 | 未使用 | `salias.hpp` 与 `import salias;` 导出 | **extras 候选**；外部使用未确认，不删除 |

## 3. 关键缺口

- 安装消费 `test/install/main.cpp` 当前直接使用 `PrioritySubscriber` 和
  `PriorityWaitPolicy`，证明核心安装归档尚未与 extras 候选解耦；该问题留给 Phase 3。
- mcpp 的 header probe 和 import test 同时覆盖规范核心路径、旧别名、Latest、LatestByKey
  和 Priority。Phase 1 只迁移旧别名并保留独立兼容测试，不在本阶段拆分 mcpp extras。
- `LatestMpmcChannel` 只支持 raw payload；直接 `Channel<Mode::LatestMpmc, Schema>` 仍提供
  typed latest 能力。不能在没有 typed latest 契约前把后者标为可删除或迁移到 raw wrapper。
- 同工作区可检索的 `gts-quote`、`gts-signal`、`gts-signal-gateway`、`gts-strategy`
  都以固定绝对路径依赖 salias，并在 11 个源码文件中引用四个 raw 别名；详细 commit、文件
  和检索命令见 [下游证据审计](channel-api-downstream-evidence.md)。这四个仓库没有使用
  `LatestByKey`、Priority 或 Ringbuffer，但仍不能代表发布包或未纳入本工作区的消费者。
- 可供审计的本地下游快照不包含发布的包消费遥测、完整外部用户清单或负责人签署，因此
  Phase 0 的外部使用门禁仍不能关闭。

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
rg -n --hidden --glob '!salias/**' --glob '!.git/**' \
  --glob '!**/build/**' --glob '!**/target/**' \
  '\b(FifoMpscChannel|FifoMpmcChannel|OrderedMpscChannel|OrderedMpmcChannel)\b' \
  /data/disk1/projects/gts-{quote,signal,signal-gateway,strategy}
```
