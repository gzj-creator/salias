# Channel API 收敛迁移说明

日期：2026-10-30（Asia/Singapore）

## 1. 当前状态

本文件描述 `v5.0.0` 的迁移路径。仓库负责人已于 2026-10-30 明确批准删除旧快捷入口；`v5.0.0`
已删除四个 raw `*Channel` 别名、三个旧 extras 转发头和九个顶层 extras 别名。

本次删除不修改共享内存 wire protocol，不升级通道控制段版本，不合并 `Channel` 与 `FixedChannel`
数据面，也不删除 `LatestByKey`、Priority 或 Ringbuffer 的 `salias::extras` 规范能力。旧类型与规范
类型原本就是同型入口，因此迁移要求重新编译，但仅因这次 API 清理不要求重建既有通道。

当前源码中不存在仍导出的 Channel 产品兼容别名。删除项在
[公开 API 清单](../api/channel-api-manifest.json)中状态为 `removed`，反向契约由
`test/api/removed_api_contract_test.cpp`、mcpp import 单元和安装消费测试锁定。

## 2. 已删除入口与源码迁移表

### 2.1 通用 Channel 快捷别名

以下名称已从头文件和 `import salias;` 删除，不再产生弃用警告，而是无法解析：

| 已删除入口 | 替代入口 | 语义 |
|---|---|---|
| `salias::FifoMpscChannel` | `salias::Channel<salias::Mode::FifoMpsc>` | 类型完全同型，per-producer FIFO、单消费者语义不变 |
| `salias::FifoMpmcChannel` | `salias::Channel<salias::Mode::FifoMpmc>` | 类型完全同型，fanout 语义不变 |
| `salias::OrderedMpscChannel` | `salias::Channel<salias::Mode::OrderedMpsc>` | 类型完全同型，跨生产者全局顺序不变 |
| `salias::OrderedMpmcChannel` | `salias::Channel<salias::Mode::OrderedMpmc>` | 类型完全同型，全局顺序与 fanout 语义不变 |

可先列出命中，再按文件执行迁移：

```bash
rg -l '\bsalias::(FifoMpsc|FifoMpmc|OrderedMpsc|OrderedMpmc)Channel\b' FILES
perl -pi -e 's/\bsalias::FifoMpscChannel\b/salias::Channel<salias::Mode::FifoMpsc>/g' FILES
perl -pi -e 's/\bsalias::FifoMpmcChannel\b/salias::Channel<salias::Mode::FifoMpmc>/g' FILES
perl -pi -e 's/\bsalias::OrderedMpscChannel\b/salias::Channel<salias::Mode::OrderedMpsc>/g' FILES
perl -pi -e 's/\bsalias::OrderedMpmcChannel\b/salias::Channel<salias::Mode::OrderedMpmc>/g' FILES
```

### 2.2 extras 顶层入口与旧 include

`LatestByKey`、Priority 和 Ringbuffer 能力仍保留，但只通过 `salias::extras` 命名空间、独立
include 根、`salias::extras` CMake target 和 `salias.extras` module 提供。以下顶层名称和三个
转发头已删除：

| 已删除入口 | 规范入口 |
|---|---|
| `<salias/latest_by_key_channel.hpp>` | `<salias/extras/latest_by_key_channel.hpp>` |
| `<salias/priority_channel.hpp>` | `<salias/extras/priority_channel.hpp>` |
| `<salias/ringbuffer.hpp>` | `<salias/extras/ringbuffer.hpp>` |
| `salias::LatestByKeyChannel` | `salias::extras::LatestByKeyChannel` |
| `salias::LatestByKeyPublisher` | `salias::extras::LatestByKeyPublisher` |
| `salias::LatestByKeySubscriber` | `salias::extras::LatestByKeySubscriber` |
| `salias::LatestByKeyMessage` | `salias::extras::LatestByKeyMessage` |
| `salias::PriorityPublisher` | `salias::extras::PriorityPublisher` |
| `salias::PrioritySubscriber` | `salias::extras::PrioritySubscriber` |
| `salias::PriorityMessage` | `salias::extras::PriorityMessage` |
| `salias::PriorityWaitPolicy` | `salias::extras::PriorityWaitPolicy` |
| `salias::Ringbuffer` | `salias::extras::Ringbuffer` |

迁移时同时完成 namespace、include、链接和 module 变更：

```bash
perl -pi -e 's{\bsalias::(LatestByKeyChannel|LatestByKeyPublisher|LatestByKeySubscriber|LatestByKeyMessage|PriorityPublisher|PrioritySubscriber|PriorityMessage|PriorityWaitPolicy|Ringbuffer)\b}{salias::extras::$1}g' FILES
perl -pi -e 's{<salias/(latest_by_key_channel|priority_channel|ringbuffer)\.hpp>}{<salias/extras/$1.hpp>}g' FILES
```

CMake 消费者链接 `salias::extras`。只使用通用 Channel、`FixedChannel` 或 `LatestMpmcChannel` 的
消费者继续链接 `salias::salias`，不要为未使用的 extras 增加链接依赖。模块消费者使用
`import salias; import salias.extras;`；仅使用核心入口时不要导入 extras。

### 2.3 直接 latest 工厂

直接 `Channel<salias::Mode::LatestMpmc>` 的 `create()` / `connect()` 在 Phase 2 已关闭。raw 和 typed
latest 都必须使用 `LatestMpmcChannel`：

| 已删除路径 | 规范入口 |
|---|---|
| `salias::Channel<salias::Mode::LatestMpmc>` 的直接创建/连接 | `salias::LatestMpmcChannel<Capacity, HighWatermarkPercent>` |
| `salias::Channel<salias::Mode::LatestMpmc, Schema>` 的直接创建/连接 | `salias::LatestMpmcChannel<Capacity, HighWatermarkPercent, Schema>` |

typed 用户必须保留第三个 `Schema` 模板参数。不能为了减少类型而改为 raw payload；该 wrapper
继续校验 schema ID、版本、payload 大小、固定容量和水位，并保留背压、动态订阅和故障语义。

## 3. 版本、弃用与回滚策略

- `v4.1.0` 是旧入口最后一个已发布 tag；四个 raw 别名和九个 extras 顶层类型在该版本标记
  `[[deprecated]]`，旧转发头和迁移表同时发布。
- `v5.0.0` 源码删除由仓库负责人于 2026-10-30 明确批准，并由后续发布授权确认，不由未采集的仓库外
  证据推断。
- 完整 ABBA、TSan、安装/module 消费和 API SHA-256 冻结已在 Phase 5 的仓库内验证中完成；`v5.0.0`
  release note、版本 tag 和发布记录已在同一发布提交闭环。
- 下游应固定并使用 `v4.1.0` 完成迁移，再升级到包含本次删除的版本。`-Werror` 工程应在
  `v4.1.0` 上先清除 `-Wdeprecated-declarations`，不要关闭告警。
- 如需短期回滚源码删除，恢复到包含兼容入口的提交或继续使用 `v4.1.0`。wire protocol 和通道
  版本未变，回滚 API 层不要求修改已有共享内存布局；应用仍需重新链接匹配的头文件和库。

## 4. 仓库外证据边界

按负责人决定，没有扫描仓库外源码消费方，也不声称其调用量、迁移成本或私有仓库零使用。
`doc/api/channel-api-phase0-evidence.json` 和签署材料仍属于仓库外证据缺口并标记为阻塞，不能用
本次仓库内验证替代。该缺口不改变已经由负责人明确批准的源码删除决定。

## 5. 验证与剩余门禁

本次源码删除至少由以下仓库内门禁验证：

- Release、ASan/UBSan 与 TSan CTest 覆盖共享 Channel、`FixedChannel`、Latest 和规范 extras 行为。
- `test/api/removed_api_contract_test.cpp` 用 `__has_include` 锁定三个旧转发头不可见；
  `tools/check_channel_api.py` 对 13 个旧类型和 3 个头文件执行逐个真实编译失败断言。
- `test/mcpp/core_import.cpp` 与 `test/mcpp/import.cpp` 实际编译 `import salias;` 和
  `import salias.extras;` 的规范消费；门禁同时核对 module export 集合与 manifest 完全一致。
- 安装消费测试验证三个旧转发头不进入安装前缀，并继续验证 core-only 与 extras 消费矩阵。
- `tools/check_channel_api.py` 校验 manifest、module、umbrella、文档和反向契约测试一致。
- `v4.1.0` 与当前树使用相同 Release 参数构建的固定槽和通用 Channel benchmark 可执行文件逐字节
  相同；5 组 A/B/B/A、每 case 每臂 10 次正式运行全部通过消息数、全量 payload 和延迟采样校验。
  原始结果、主机信息和哈希见
  [Channel API ABBA 证据](../benchmarks/2026-10-31-channel-api-abba/README.md)。
- API 冻结基线中 `release_tag` 为 `v5.0.0`，绑定 53 个公开/安装文件、manifest、module 和 CMake/mcpp
  导出面；逐文件 SHA-256 与聚合 digest 由 `salias_channel_api_documentation_check` 自动校验。
  说明见 [Channel API 冻结基线](../api/channel-api-freeze.md)。

Phase 5 已随 `v5.0.0` 发布完成。Phase 0 未采集的仓库外使用证据仍是独立审计缺口，不阻塞本版本
基于负责人授权的源码收敛，也不能被解释为全局零使用。
