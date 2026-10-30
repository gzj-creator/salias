# Salias Channel API 收敛计划

日期：2026-10-30（Asia/Singapore）

状态：**Phase 0 本地审计与 Phase 1-3 完成；Phase 4 源码删除在 `[Unreleased]` 完成，正式发布门禁未完成；Phase 5 未开始**

## 1. 目标

本计划只收敛公开 Channel API 和依赖边界，不重写数据面。目标是把“用户必须理解并选择的
Channel 产品”收敛到少量稳定入口，同时保留当前性能路径和线上语义。

完成后应满足：

1. 新用户只需要区分“通用可靠通道”和“固定布局快速通道”两个流式通道入口。
2. 每种能力只有一个推荐主入口，不再同时用别名、便捷类和直接调用 `Channel<Mode>` 三套方式宣传。
3. `LatestByKey`、Priority、进程内 `Ringbuffer` 等非核心能力不进入默认快速入口，也不隐式增加核心库依赖。
4. 通用 `Channel` 与 `FixedChannel` 继续使用独立数据面，不重新合成带运行时分支的万能模板。
5. 所有删除都经过兼容窗口、下游迁移验证和主版本门禁，不能在 patch/minor 中静默破坏源码兼容。
6. 收敛前后核心热路径协议、性能和正确性不退化。

衡量结果不是单纯减少类型数量，而是减少用户第一次选型时的有效选项、重复的规范入口和核心库的
可选依赖。

## 2. 非目标

- 不把通用 `Channel` 和 `FixedChannel` 合并成一个运行时切换的数据面。
- 不在本计划中执行 `Fast*Ring` 到 `SpscRing`/`MpscRing` 等最终重命名；该事项仍由
  [Fast Ring 性能替换计划](2026-10-29-fast-ring-performance-replacement.md)控制。
- 不修改共享内存 wire protocol，不为 API 收敛强制升级现有通道 ABI。
- 不删除 raw 变长 payload 路径；它仍是 `FixedChannel` 无法覆盖的通用入口。
- 不删除 schema-bound typed 路径；它承担跨进程布局和版本校验。
- 不以减少 API 为理由弱化可靠背压、schema 检查、订阅策略或故障测试。
- 不在没有下游使用证据时直接删除 `LatestByKey` 或 Priority；默认方案是隔离到 extras。

## 3. 当前基线

### 3.1 用户可见能力

| 能力 | 当前入口 | 数据面/语义 | 初步处置 |
|---|---|---|---|
| 通用可靠流 | `Channel<Mode, Schema>` | per-producer magic-ring；FIFO 或 Ordered | **核心保留** |
| 通用 raw 流 | `Channel<Mode>` | 变长 payload、动态订阅和借用消息 | **核心保留** |
| 通用便捷别名 | `FifoMpscChannel` 等四个别名 | 与 `Channel<Mode>` 完全同型 | **兼容后删除** |
| 固定快速流 | `FixedChannel<Topology, Schema, Capacity, ...>` | 固定 slot、固定参与者、可靠 FIFO | **核心保留** |
| 最新窗口 | `LatestMpmcChannel<Capacity, Watermark>` | 有界 latest fanout、可丢旧或 Reliable | **核心主入口** |
| 直接 latest 模式 | `Channel<Mode::LatestMpmc>` | 与上一行同引擎，但绕过静态配置门禁 | **移除公开工厂；仅保留内部数据面** |
| 按 key 最新值 | `LatestByKeyChannel<Key, Value, Capacity, Version>` | 进程共享锁、key 表、快照和事件日志 | **extras 候选** |
| 优先级组合 | `PriorityPublisher<Mode...>` / `PrioritySubscriber<Mode...>` | 多通道路由和加权轮询 | **extras 候选** |
| 本地队列 | `Ringbuffer<T, Capacity, Watermark>` | 进程内单线程有界丢旧队列 | **extras 候选** |

`Publisher`、`Subscriber`、`PublishClaim`、`LatestByKeyMessage` 等端点类型由通道返回，不是用户需要
独立选型的产品；文档应继续保留其契约，但不把它们与 Channel 产品并列宣传。

### 3.2 收敛前复杂度证据

- 通用引擎公开 5 个 `Mode`：`FifoMpsc`、`FifoMpmc`、`OrderedMpsc`、`OrderedMpmc`、
  `LatestMpmc`。
- `FixedChannel` 公开 4 个 topology：`Spsc`、`Mpsc`、`Spmc`、`Mpmc`。
- 同时存在 4 个 raw `*Channel` 别名和 `Channel<Mode>` 规范形式。
- 收敛前 `LatestMpmc` 同时存在 wrapper 和直接 `Channel<Mode::LatestMpmc>` 两条公开创建路径。
- `LatestByKeyChannel` 的公开头与实现约 1,220 行，专项测试约 1,067 行。
- Priority 的公开头约 511 行，专项测试约 564 行。
- 收敛前统一伞头文件 `salias/salias.hpp` 同时导出核心通道、Latest、LatestByKey、Priority 和
  Ringbuffer；Phase 3 已将后三者拆到独立 extras 边界。

### 3.3 与 Fast Ring 计划的边界

本计划建立前仓库没有正式的 Channel API 淘汰计划。Fast Ring 计划明确要求通用 `Channel` 继续保留
Ordered、Latest、TTL、动态订阅和恢复语义，见
[Fast Ring 性能替换计划 §2](2026-10-29-fast-ring-performance-replacement.md)。因此本计划不能把
“引入 FixedChannel”理解为“删除通用 Channel”。

## 4. 目标公开面

### 4.1 核心流式通道

```cpp
// 通用、动态、变长或需要 Ordered/Latest 的场景
using Dynamic = salias::Channel<salias::Mode::FifoMpsc, QuoteSchema>;

// 固定 payload、固定参与者、追求最低热路径开销的场景
using Fast = salias::FixedChannel<
    salias::FixedTopology::Spsc,
    QuoteSchema,
    4096>;
```

这是两个数据面、两个产品入口，而不是两个可以运行时互换的实现。文档必须明确二者在动态参与者、
变长 payload、Ordered、TTL、Latest 和容量语义上的边界。

### 4.2 核心 latest fanout

```cpp
using Quotes = salias::LatestMpmcChannel<1u << 20, 90, QuoteSchema>;
```

`LatestMpmcChannel` 把容量、水位和可选 schema 放在类型中，并在 `connect()` 时校验共享配置。
直接 `Channel<Mode::LatestMpmc>` 的 `create()`/`connect()` 已关闭；底层 `Channel` 数据面继续由
wrapper 使用，因此本决定不修改 wire protocol，也不复制 latest 引擎。

### 4.3 extras

目标是增加明确的 extras 边界，而不是立即删除实现：

```cpp
#include <salias/extras/latest_by_key_channel.hpp>
#include <salias/extras/priority_channel.hpp>
#include <salias/extras/ringbuffer.hpp>
```

extras 应满足：

- 独立安装头文件和可选 CMake target。
- 不包含在核心 `salias/salias.hpp` 中，除非构建显式启用兼容聚合头。
- 核心 `salias` library 不因未使用 extras 而链接 keyed table 的实现。
- C++23 module 提供独立 export module 或明确的兼容聚合方式。
- 测试、benchmark 和样例使用 extras 自身的依赖边界。

## 5. 收敛原则

1. **一种能力一个主入口。** 重复别名先废弃，再在主版本删除。
2. **数据面不合并。** 通用和固定引擎的共存成本低于把通用分支重新带回固定槽热路径。
3. **先隔离，后删除。** 外围能力先移动到 extras 并验证安装/模块/跨进程使用，再决定彻底删除。
4. **端点类型不是产品选择。** 用户选择 Channel；Publisher/Subscriber 由 Channel 返回。
5. **源码兼容与 wire 兼容分开。** API 迁移要求重编译，但不应无故要求重建现有共享内存通道。
6. **文档只展示推荐路径。** 兼容入口保留在迁移说明和 API 参考中，不占据快速开始和功能矩阵。
7. **仓库内闭环。** 接口收敛以 salias 自身的测试、安装、module 和文档为依据；仓库外消费方自行适配，
   不把源码扫描结果作为当前门禁。

## 6. 执行清单

### Phase 0：基线与决策

- [x] 创建并落盘本计划。
- [x] 盘点当前 Channel、Mode、FixedTopology、别名、wrapper 和 extras 候选。
- [x] 记录 `LatestByKey`、Priority、Ringbuffer 的实现与测试规模。
- [x] 确认本计划与 Fast Ring 重命名计划相互独立，不合并两个数据面。
- [x] 固化“minor 只废弃、major 才删除源码 API”的版本策略。
- [ ] 收集仓库外下游使用证据，确认 `LatestByKey`、Priority、Ringbuffer 的实际用户和迁移成本。
- [ ] 负责人签署核心保留表、extras 候选表和 `LatestMpmc` 唯一主入口方案。

**Phase 0 退出条件**：所有待删或待移能力都有明确处置，且没有把正式功能误标为兼容层。

本 session 已对 `test/`、`bench/`、`example/`、安装消费、mcpp 和 module 导出完成仓库内引用
审计，结果见[仓库内使用矩阵](../api/channel-api-usage-matrix.md)。2026-10-30 用户明确决定不再
检查仓库外源码消费方，下游按其依赖升级窗口自行适配；因此本计划不采集、不推断也不伪造发布包
遥测、私有仓库使用或迁移成本。两个仓库外/签署复选框继续保持未勾选，但它们不再阻塞后续
不把仓库外证据作为本地源码收敛门禁。Phase 4 删除由负责人于 2026-10-30 另行明确批准；外部证据
复选框仍保持未完成。

### Phase 1：规范入口与文档冻结

- [x] 新增公开 API 清单，按 core、compat、extras、removed 四种状态记录每个 symbol。
- [x] README 快速开始只保留 `Channel<Mode, Schema>`、`FixedChannel` 和
  `LatestMpmcChannel` 三个核心示例。
- [x] `doc/11-L7-api.md` 将四个 raw 别名降为“兼容入口”，统一推荐 `Channel<Mode>`。
- [x] 示例、benchmark 和新增测试逐步改用规范入口；兼容测试单独保留旧别名。
- [x] 为四个 `*Channel` 别名增加弃用策略和迁移说明；本阶段不删除。
- [x] 明确 raw/typed 不是两套产品，而是同一 Channel 的 payload 模式。
- [x] 评估 `Schema<T, Id, Version>` 辅助模板，只有在保持显式 schema ID/version 且不增加运行时
  成本时采用。
- [x] 建立文档检查，禁止快速开始重新引入非主入口。

**Phase 1 退出条件**：新用户路径只有一个规范写法；兼容别名仍有测试但不再出现在主教程中。

本 session 已满足 Phase 1 退出条件。`Schema<T, Id, Version>` 采用为无数据成员、无运行时路径的
显式声明辅助模板；schema ID/version 仍是模板参数，没有引入默认版本。四个兼容别名使用
`[[deprecated]]` 保留源码兼容，并由专项测试锁定其同型关系。Phase 0 的两个外部/签署门禁仍未完成，
因此不得据此删除兼容入口；后续用户明确要求下游自行适配，允许在不删除任何 API 的前提下继续完成
本地 extras 边界迁移。

### Phase 2：Latest 主入口收敛

- [x] 确认 `LatestMpmcChannel` 是公开 latest fanout 的唯一入口。
- [x] 盘点仓库内直接使用 `Channel<Mode::LatestMpmc>` 的测试、benchmark、模块导出和安装消费。
- [x] 决定 typed latest 属于核心需求。
- [x] 为 `LatestMpmcChannel` 增加可选 `Schema` 参数，保留 schema ID、版本、payload 大小和静态配置校验。
- [x] 关闭直接 `Channel<Mode::LatestMpmc>` 的 `create()`/`connect()`，不再暴露第三条路径。
- [x] 增加测试，确保 wrapper 与底层模式在容量、水位、策略、schema 和连接校验上完全一致。
- [x] 保留底层 latest 数据面供 wrapper 使用，不修改共享内存协议或通道版本。

**Phase 2 退出条件**：Latest 只有一个推荐入口，且不存在静态配置校验旁路。

本 session 在用户“下游自行适配”的明确决定下完成该收敛。raw 和 typed latest 都使用
`LatestMpmcChannel<Capacity, HighWatermarkPercent[, Schema]>`；直接 Channel 工厂由编译期约束关闭，
而不是删除底层引擎或升级 wire protocol。

### Phase 3：extras 边界

- [x] 设计 `salias::extras` 命名空间、include 根目录和可选 CMake target。
- [x] 将 `LatestByKeyChannel`、`LatestByKeyPublisher`、`LatestByKeySubscriber` 和
  `LatestByKeyMessage` 移入 extras。
- [x] 将 `PriorityPublisher`、`PrioritySubscriber`、`PriorityMessage` 和
  `PriorityWaitPolicy` 移入 extras。
- [x] 将 `Ringbuffer` 移入 extras，或作为独立的 header-only utility target 发布。
- [x] 将 keyed table 实现从核心 `salias` library 拆出，确保未使用 extras 的消费者不链接相关实现。
- [x] 更新 `mcpp.toml`、`src/salias.cppm`、安装规则、package config 和 header probes。
- [x] 增加“只链接 core 不包含 extras”的安装消费测试。
- [x] 增加“extras 单独安装和跨进程运行”测试。
- [x] 更新 README、API 文档、示例、benchmark 和性能报告中的 include/link 路径。
- [x] 提供旧 include 到新 include 的迁移表和一版兼容转发头。

**Phase 3 退出条件**：core 构建不依赖 extras 实现；extras 可独立安装、链接、模块导入并通过测试。

Phase 3 已完成。核心 umbrella、`import salias;`、`salias::salias` 和 install `core` component 不再
包含 extras；`salias::extras`、`import salias.extras;` 和 install `extras` component 可独立消费。
旧三个 include 与九个顶层类型仍作为 `[[deprecated]]` 兼容入口发布，本阶段不删除。

### Phase 4：兼容别名与重复入口删除

- [ ] 在弃用窗口至少一个 minor 后，确认构建警告、release note 和迁移示例已发布。
  - [x] `v4.1.0` 已发布四个 raw 别名的弃用标记、同型兼容测试和迁移/codemod 示例。
  - [x] 当前源码在 GCC 下实际产生四个 raw 别名和九个 extras 顶层别名的
    `-Wdeprecated-declarations` 警告；规范入口在同一 `-Werror` 编译参数下通过。
  - [ ] `v4.1.0` 之后尚无新的 release minor；Phase 3 extras 变更仍是未打 tag 的 `[Unreleased]`。
  - [ ] 包含 extras 边界、警告样例和删除/回滚说明的 release note 尚未发布。
- [x] 删除 `FifoMpscChannel`、`FifoMpmcChannel`、`OrderedMpscChannel` 和
  `OrderedMpmcChannel` 别名，或由负责人明确批准永久保留并更新本计划。
- [x] 按 Phase 2 决策关闭直接 `Channel<Mode::LatestMpmc>` 的公开创建/连接工厂。
- [x] 删除三个 extras 转发头及其九个顶层别名，同时保留 `salias::extras` 规范能力。
- [x] 将 `salias/salias.hpp` 收敛为核心聚合头，不再默认包含 extras。（作为 Phase 3 边界调整提前完成）
- [x] 更新 C++23 module export，移除核心 module 中的四个 raw 快捷别名；核心 module 仍不包含 extras。
- [x] 清理文档中的重复入口、废弃命名和双份示例。快速开始与主路径只展示规范入口；旧名称仅保留在
  API 参考、迁移/回滚说明、removed 契约测试和明确的历史表中。
- [x] 确认没有内部代码通过 compat 别名遮蔽规范路径。`tools/check_channel_api.py` 扫描
  `test/`、`bench/`、`example/`，仅允许 removed 契约测试中的旧 include 负向检查。
- [x] 新增 `test/api/removed_api_contract_test.cpp` 锁定三个旧 include 不可见，并用
  `tools/check_channel_api.py` 对 13 个旧类型执行逐个真实编译失败断言；mcpp header/import 测试
  实际验证规范 module 消费，门禁精确核对 module export 集合。
- [x] CMake 不再安装或导出三个旧转发头；安装消费测试反向断言它们不存在。
- [x] 仓库负责人于 2026-10-30 明确批准在 `[Unreleased]` 执行源码删除，不以未采集的仓库外证据推断。

**Phase 4 退出条件**：核心公开面中不存在同语义双入口；所有删除项在正向使用、module 和安装测试中清零。

Phase 4 源码删除已完成。最后发布 tag 仍是 `v4.1.0`，`v4.1.0` 之后尚无新的 release minor，也尚未
发布包含 extras 边界和删除/回滚说明的 release note，因此列表首项及其发布子项仍保持未完成。
本 session 不打主版本 tag；正式 `5.0.0` 发布、完整性能/TSan/hash 冻结和签署统一下沉到 Phase 5。

### Phase 5：最终验证与发布

- [ ] Release、ASan/UBSan、TSan 全套 CTest 通过。
- [ ] `salias_install_consumer`、`mcpp header/import`、仅 core 和 core+extras 两种消费矩阵通过。
- [ ] 对固定槽热路径和通用 Channel 分别执行收敛前后 ABBA；证明未因 include/link 拆分改变热路径。
- [ ] 若无数据面代码变化，保存二进制和符号差异证据；不能仅凭“理论上没有影响”关闭。
- [ ] 更新 `CHANGELOG.md`、`docs/release_note.md` 和主版本迁移说明。
- [ ] 提供按 symbol 的自动迁移命令或 codemod 清单。
- [ ] 冻结最终公开 API 清单和 SHA-256，作为下一版本兼容基线。
- [ ] 负责人签署 Phase 5，并决定是否打主版本 tag。

**Phase 5 退出条件**：源码、安装、模块、文档、性能和发布证据全部闭环，且主版本迁移说明可由下游
独立执行。

## 7. 版本与兼容策略

| 版本 | 允许的变更 |
|---|---|
| 已发布 `v4.1.0` | 增加弃用标记、规范文档和兼容转发头；拆分 extras target、install component 和 module；关闭被 wrapper 完全替代的直接 latest 工厂 |
| 当前 post-4.1 `[Unreleased]` | 经负责人明确批准，删除四个 `*Channel` 快捷别名、九个顶层 extras 别名和三个转发头；规范 extras 能力保留 |
| 下一个 `5.0.0` release | 将已完成源码删除纳入正式主版本发布、release note、性能/TSan/hash 门禁和 tag；不借机升级 wire protocol |
| 未来 major | 只有出现明确替代能力且经过一个 minor 弃用窗口后，才允许删除 extras 中的能力 |

wire protocol 与源码 API 版本分别判断。仅移动 include 或删除别名时，不借机修改共享控制段版本；
反之，未来协议变化也不能冒充 API 收敛。

## 8. 验证门禁

### 8.1 API 与文档

- 公开 API 清单中每个 symbol 只有一个状态：core、compat、extras、removed。
- README 快速开始不出现 compat 别名或 extras。
- `salias/salias.hpp` 的 include graph 不包含 extras 实现。
- C++23 module 和 header 消费的可见符号集合一致。

### 8.2 正确性

- 所有现有通用 Channel、FixedChannel、Latest、LatestByKey 和 Priority 测试在迁移前后通过。
- removed include 契约和 13 个真实编译失败断言锁定旧入口不可见；mcpp header/import 只导出
  manifest 中的规范集合。
- 连接校验、schema mismatch、背压、慢消费者、进程退出和 fork 约束不因移动文件而改变。

### 8.3 性能

- 收敛不得修改固定槽 slot 布局、completion protocol、cursor release 或 lane 轮询。
- 若数据面零改动，提交二进制/符号对比和至少一轮完整构建证据。
- 若链接或内联边界发生变化，执行同机 ABBA；固定通道和通用通道分别报告吞吐与 p50/p99。
- 未通过性能门禁时回退依赖拆分，不用更慢的 extras 边界换形式上的整洁。

## 9. 风险与回滚

| 风险 | 缓解 |
|---|---|
| 下游只使用旧别名 | 保留一个 minor 的转发头和弃用期，提供 codemod |
| extras 拆分破坏链接 | 先增加独立 target 并并存，验证安装消费后再从核心库移除实现 |
| module 导出与 header 不一致 | 使用共享 symbol 清单并运行双消费测试 |
| Latest 唯一入口丢失 typed 能力 | Phase 2 先补契约和测试，禁止先删后补 |
| 拆分导致编译/内联变化 | 对比符号和性能；必要时只拆 include，不拆目标文件 |
| 清理范围扩张成数据面重构 | 本计划禁止修改协议和热路径；另立计划处理性能改动 |

每一步使用独立提交。任何删除都必须在 extras/兼容入口已发布且验证通过后进行；若主版本迁移失败，
恢复兼容转发层并保留新建的依赖边界。

## 10. 执行记录

2026-10-30 用户将仓库外源码消费方明确移出本收敛任务。下列提及 `gts-*`、GitHub、发行包或部署
ELF 的历史记录只保留为当时已产生的审计痕迹，不再作为接口取舍、Phase 1 或 Phase 2 的验收门禁；
本 session 不再执行或扩展此类检查。

| 日期 | Phase | 结果 | 证据 |
|---|---|---|---|
| 2026-10-30 | 0 | 计划落盘；确认当前无正式 API 收敛计划 | 本文件 |
| 2026-10-30 | 0 | 完成公开入口盘点：5 个 Mode、4 个 FixedTopology、4 个 raw 别名、2 条 Latest 路径和 3 个 extras 候选 | `src/salias/include/salias/{config.hpp,fixed_channel.hpp,channel.hpp,latest_channel.hpp,latest_by_key_channel.hpp,priority_channel.hpp,ringbuffer.hpp}` |
| 2026-10-30 | 0 | 记录外围规模：LatestByKey 实现约 1,220 行、专项测试约 1,067 行；Priority 实现约 511 行、专项测试约 564 行 | `wc -l src/salias/include/salias/latest_by_key_channel.hpp src/salias/src/latest_by_key.cpp src/salias/include/salias/priority_channel.hpp test/api/latest_by_key_test.cpp test/api/priority_channel_test.cpp` |
| 2026-10-30 | 0 | 完成 test/bench/example/install/mcpp 引用矩阵；确认安装消费仍直接依赖 Priority，mcpp 仍聚合导出 extras 候选，留待 Phase 3 | `doc/api/channel-api-usage-matrix.md`；`test/install/main.cpp`、`test/mcpp/{header_probe,import}.cpp`、`src/salias.cppm` |
| 2026-10-30 | 0 | 采集同工作区四个 salias 下游仓库源码快照：`gts-quote`、`gts-signal`、`gts-signal-gateway`、`gts-strategy` 均在 11 个文件中共 48 行引用 `FifoMpscChannel`/`FifoMpmcChannel`；未检索到 `LatestByKey`、Priority 或 Ringbuffer 引用，但不能据此断言全局零使用 | `/data/disk1/projects/{gts-quote,gts-signal,gts-signal-gateway,gts-strategy}`；commit 快照与复核命令见 `doc/api/channel-api-downstream-evidence.md` |
| 2026-10-30 | 0 | 四个下游仓库的 `mcpp.toml` 均启用 `-Werror`；代表性 `FifoMpscChannel` 消费在同等告警配置下因 `-Werror=deprecated-declarations` 编译失败，确认迁移是采用 4.1 的前置条件，不是零成本重编译 | `rg -n 'cxxflags|Werror' /data/disk1/projects/{gts-quote,gts-signal,gts-signal-gateway,gts-strategy}/mcpp.toml`；代表性 `c++ -std=c++23 ... -Werror -fsyntax-only`，exit 1 |
| 2026-10-30 | 0 | 将四个已知本地下游仓库的 48 行、11 个 raw `*Channel` 源码引用全部迁移到 `Channel<Mode>`；当前 C++ 源码命中为 0，保留 publisher/subscriber 端点、MPSC/MPMC 分支、背压、订阅槽位和 wire v15 语义 | `/data/disk1/projects/{gts-quote,gts-signal,gts-signal-gateway,gts-strategy}`；迁移矩阵与复核命令见 `doc/api/channel-api-downstream-evidence.md` |
| 2026-10-30 | 0 | 已知下游验证分层完成：gts-signal Release workspace 与 `tools/check.py --skip-e2e` 通过，重建 `.so` 的 strip 后内容与旧产物一致；gts-quote transport 与 consumer 模块、gts-strategy SHM/测试对象单编通过；另外三个仓库完整构建均被相邻 Galay HTTP task API 既有错误阻塞，未伪报为通过 | `mcpp build --offline --release --jobs 4 --workspace`（gts-signal）；`PYTHONPATH="$PWD/src/python:$PWD" python3 tools/check.py --skip-e2e`；`ninja -C target/x86_64-linux-gnu/111162597052f8a3 obj/gts-quote-tools/consumer/consumer.m.o`；strip 前后 SHA-256 与目标级 `-Werror` object compile；gts-quote salias lock 专项检查 |
| 2026-10-30 | 0 | 已同步 `gts-quote`、`gts-signal`、`gts-signal-gateway` 的中央 RAG 镜像；`gts-strategy` 按文档门禁继续保持未建镜像 | `python3 tools/check_rag_corpus.py`：58 个 Markdown、927 个链接或来源引用，隔离检查通过；镜像位于 `/data/disk1/projects/gts-ai/docs/projects/` |
| 2026-10-30 | 0 | 复核本地发行面和部署产物：发行报告确认四个直接 salias 消费者；九个相关部署 ELF 均含 `LatestByKeyStorage` 实现符号，另两个 `gts-inner-api` ELF 不含。该符号只证明默认静态链接，不证明 API 使用 | `/data/disk1/quote/reports/release-20261025-01/`；`sha256sum`、`nm -C --defined-only` 全量结果见 `doc/api/channel-api-phase0-evidence.json` |
| 2026-10-30 | 0 | 补充公开外部渠道可达性：完整扫描 GitHub 公开账户中除 salias 外的 17 个可下载仓库，未发现 salias API 引用；Galay 的 `Ringbuffer` 命中属于其自身 `B10-Ringbuffer` benchmark。另外定向检查 `agents-manager` 的 Cargo/package manifests 和 `galay-http` 的 CMake/README，均无 salias 依赖，但完整源码因归档/连接超时未扫完。GitHub code search/traffic 仍需认证，私有仓库与包遥测不可得 | GitHub `gzj-creator` 公开仓库列表、`codeload.github.com` 默认分支归档、GitHub 文件页定向检查；限制记录于 `doc/api/channel-api-phase0-evidence.json` |
| 2026-10-30 | 0 | 将已采集证据整理为机器可读 JSON 并生成逐项签署草案；复选框按证据状态保持未完成，不把草案、静态符号或本地快照冒充已签或全局使用数据 | `doc/api/channel-api-phase0-evidence.json`；`doc/api/channel-api-phase0-signoff.md` |
| 2026-10-30 | 0 | 仓库外完整用户清单、发布包消费遥测和迁移成本确认仍不可获得；负责人签署待办，Phase 0 两个对应复选框保持未完成 | `doc/api/channel-api-downstream-evidence.md` |
| 2026-10-30 | 1 | 建立并更新机器可读公开 API 清单：30 core、4 compat、9 extras、1 removed；每项含代码位置、状态、替代入口、header/module/install/mcpp 导出状态 | `doc/api/channel-api-manifest.json`；`python3 tools/check_channel_api.py` 输出 `Channel API manifest OK: 30 core, 4 compat, 9 extras, 1 removed` |
| 2026-10-30 | 1 | README 快速开始冻结为 `Channel<Mode, Schema>`、`FixedChannel`、`LatestMpmcChannel` 三个核心入口；四个别名与直接 latest 模式移入兼容说明；raw/typed 明示为同一 Channel 的 payload 模式 | `README.md`、`doc/11-L7-api.md` |
| 2026-10-30 | 1 | 示例、benchmark、mcpp 和非兼容专项测试迁移到 `Channel<Mode>`；四别名只保留在 `channel_alias_compat_test.cpp`；别名标 `[[deprecated]]`，本版本不删除 | `rg -n 'FifoMpscChannel|FifoMpmcChannel|OrderedMpscChannel|OrderedMpmcChannel' test bench example test/mcpp`；兼容测试、`doc/migration/channel-api-convergence.md` |
| 2026-10-30 | 1 | 评估并采用 `Schema<T, Id, Version>`：静态断言非零显式 ID/version，空对象、无运行时成本；typed Channel 与 mcpp 头/模块消费均覆盖 | `src/salias/include/salias/message.hpp`、`test/api/schema_helper_test.cpp`、`test/mcpp/typed_schema.hpp` |
| 2026-10-30 | 1 | 新增文档门禁：校验 symbol 状态唯一性、代码位置、module/cpp 导出一致性、四别名兼容文档、README 快速开始禁入项与版本一致性 | `tools/check_channel_api.py`、`test/CMakeLists.txt` 的 `salias_channel_api_documentation_check` |
| 2026-10-30 | 1 | Release 构建与 25/25 CTest 通过，其中安装消费、文档检查、别名兼容和 Schema helper 用例均通过；ASan/UBSan 24/24 通过；mcpp 模块/头文件混合消费通过 | `cmake --build --preset release -j 4`；`ctest --test-dir build/release --output-on-failure -j 2`；`cmake --build --preset debug-asan-ubsan -j 4`；`ctest --preset debug-asan-ubsan --output-on-failure -j 2`；`mcpp build --offline --release --jobs 4`；`mcpp run test_mcpp_import --offline --release --locked --features mcpp-tests`；`python3 tools/check_mcpp.py`；`git diff --check` |
| 2026-10-30 | 2 | 关闭直接 `Channel<Mode::LatestMpmc>` 的 `create()`/`connect()`；raw 与 typed latest 统一为 `LatestMpmcChannel<Capacity, Watermark[, Schema]>`，wrapper 继续驱动同一底层 Channel 数据面 | `src/salias/include/salias/{channel,latest_channel}.hpp`、`src/salias/src/channel.cpp`、`test/api/channel_api_test.cpp` |
| 2026-10-30 | 2 | 补齐 wrapper 的类型/配置契约：创建覆盖容量和水位，连接校验容量、水位、schema ID、版本和 payload 大小；编译期断言直接 Channel 工厂不可调用，并覆盖 subscription policy、TTL、lag、poll、背压和跨进程 lease 测试 | `test/api/channel_api_test.cpp`、`test/api/poll_storage_contract_test.cpp`、`test/api/allocation_contract_test.cpp` |
| 2026-10-30 | 1-2 | README、API 参考、迁移表、仓库内使用矩阵和 manifest 同步为唯一 latest 入口；安装消费和 mcpp 模块/头文件消费均改为 typed wrapper | `README.md`、`doc/11-L7-api.md`、`doc/migration/channel-api-convergence.md`、`doc/api/channel-api-usage-matrix.md`、`test/install/main.cpp`、`test/mcpp/{header_probe,import}.cpp` |
| 2026-10-30 | 2 | Release 25/25、ASan/UBSan 24/24、安装消费和 mcpp header/import 通过；本轮只改公开模板/mcpp 测试和文档，未改共享内存 wire protocol、通道版本、固定槽热路径或 `Fast*Ring` 名称，因此未执行性能 ABBA | `cmake --build --preset release -j 4`；`ctest --test-dir build/release --output-on-failure -j 2`；`cmake --build --preset debug-asan-ubsan -j 4`；`ctest --preset debug-asan-ubsan --output-on-failure -j 2`；`mcpp build --offline --release --jobs 4`；`mcpp run test_mcpp_import --offline --release --locked --features mcpp-tests`；`python3 tools/check_mcpp.py`；`python3 tools/check_channel_api.py`；`git diff --check` |
| 2026-10-30 | 3 | 建立 `salias::extras` include 根、`salias_extras` / `salias::extras` target、`import salias.extras;` module 和 `core` / `extras` install components；核心 umbrella 与 `import salias;` 不再导出 extras | `src/salias/include/salias/extras/`、`src/salias_extras.cppm`、`src/salias/CMakeLists.txt`、`CMakeLists.txt`、`cmake/saliasConfig.cmake.in` |
| 2026-10-30 | 3 | LatestByKey 四个类型、Priority 四个类型和 Ringbuffer 迁入 `salias::extras`；三个旧 include 与九个旧顶层类型保留为 `[[deprecated]]` 转发入口，专项兼容测试锁定同型关系 | `src/salias/include/salias/extras/**`、旧三个公开头、`test/api/extras_compat_test.cpp` |
| 2026-10-30 | 3 | keyed table 私有存储和实现从核心库拆出；core 归档无 `LatestByKeyStorage` 符号，extras 归档保留 `create_latest_by_key_storage` | `src/salias/include/salias/extras/detail/latest_by_key_storage.hpp`、`src/salias/src/latest_by_key.cpp`、`nm -C build/release/src/salias/libsalias{,_extras}.a` |
| 2026-10-30 | 3 | test、bench、install 与 mcpp 主路径迁移到规范 extras 入口；新增 core-only import、独立 extras 安装/跨进程、core-only 无 extras artifact 检查，并把 Ringbuffer 测试拆到独立 extras target | `test/CMakeLists.txt`、`test/install/extras/`、`cmake/install_consumer_test.cmake`、`test/mcpp/core_import.cpp`、`test/ring/ringbuffer_test.cpp` |
| 2026-10-30 | 3 | README、L7 API、L1 ring 文档、迁移表、仓库内使用矩阵和机器可读 manifest 同步 extras 边界；manifest 校验为 30 core、13 compat、9 extras、1 removed，core module 33、extras module 9 | `README.md`、`doc/{5-L1-ring,11-L7-api}.md`、`doc/migration/channel-api-convergence.md`、`doc/api/channel-api-{manifest,usage-matrix}.*`、`python3 tools/check_channel_api.py` |
| 2026-10-30 | 3 | Release 27/27 与 ASan/UBSan 26/26 通过；core-only 安装消费、extras 安装/跨进程、mcpp core-only import、core+extras import 和两个独立 mcpp 消费者均通过 | `cmake --build --preset release -j 4`；`ctest --test-dir build/release --output-on-failure -j 2`；`cmake --build --preset debug-asan-ubsan -j 4`；`ctest --preset debug-asan-ubsan --output-on-failure -j 2`；`mcpp run test_mcpp_core_import ...`；`mcpp run test_mcpp_import ...`；`python3 tools/check_mcpp.py` |
| 2026-10-30 | 3 | 数据面 TU 相对 HEAD 零差异；固定槽对象哈希保持 `45e6affb...`，通用 Channel 对象因公开头输入变化重采集为 `c8dcffd9...`；smoke 为 FixedChannel 1,000,000 条校验、`7.73244 Mmsg/s`，latest p50 `186 ns`，LatestByKey p50 `300 ns`。Google Benchmark 依赖库为 DEBUG，数值只作冒烟证据，完整 ABBA 留 Phase 5 | `git diff --quiet HEAD -- src/salias/src/channel.cpp src/salias/src/fixed_channel.cpp`（exit 0）；`sha256sum`；`build/release/bench/{salias_fixed_channel_poll_compare,salias_bench_round2,salias_bench_latest_by_key}` |
| 2026-10-30 | 4 | 删除门禁预检确认尚未满足兼容窗口：最后 tag 为 `v4.1.0`，Phase 3 extras 边界提交 `efcfc8c` 尚未形成新的 release minor；同时存在待签署和 release note 缺口，且本 session 明确禁止主版本删除 | `git log --oneline --decorate`；`git tag --sort=-version:refname`；`doc/migration/channel-api-convergence.md` §5 |
| 2026-10-30 | 4 | 实际验证弃用警告：四个 raw 别名和九个 extras 顶层别名在 GCC `-Wdeprecated-declarations` 下均产生警告；同一 `-Werror` 参数下规范 `Channel<Mode>`、`LatestMpmcChannel` 和 `salias::extras::*` 路径通过 | `c++ -std=c++23 -Isrc/salias/include -Isrc -x c++ -fsyntax-only -Wdeprecated-declarations -`；规范路径另加 `-Werror`（exit 0） |
| 2026-10-30 | 4 | 文档重复入口检查和仓库内 compat 使用审计完成；非兼容 `test/bench/example` 不再使用旧 Channel/extras 顶层名称，清单门禁继续通过 | `python3 tools/check_channel_api.py`；`rg -n 'salias::(FifoMpscChannel|FifoMpmcChannel|OrderedMpscChannel|OrderedMpmcChannel|LatestByKey|Priority|Ringbuffer)' test bench example` |
| 2026-10-30 | 4 | 负责人批准后删除四个 raw 别名、九个顶层 extras 别名和三个转发头；核心 module 只保留 29 个规范导出，manifest 更新为 30 core / 0 compat / 9 extras / 14 removed | `src/salias/include/salias/channel.hpp`、`src/salias.cppm`、`CMakeLists.txt`、`doc/api/channel-api-manifest.json`；`python3 tools/check_channel_api.py` |
| 2026-10-30 | 4 | removed 门禁使用逐个真实编译失败而非不可解析名称的伪 `requires`：13 个旧类型和 3 个旧 include 均单独编译并确认失败；旧转发头安装反向断言、README/API/迁移文档同步通过 | `python3 tools/check_channel_api.py --compiler /usr/bin/c++`：16 个负向编译检查；Release 26/26、ASan/UBSan 25/25 |
| 2026-10-30 | 4 | 安装与 module 消费闭环：core-only、core+extras install consumer、`mcpp build`、core-only import、core+extras import 和两个独立 shared-path consumer 均通过 | `ctest --test-dir build/release --output-on-failure -j 2`；`ctest --preset debug-asan-ubsan --output-on-failure -j 2`；`mcpp build --offline --release --jobs 4`；`mcpp run test_mcpp_core_import --offline --release --locked --features mcpp-tests`；`mcpp run test_mcpp_import --offline --release --locked --features mcpp-tests`；`python3 tools/check_mcpp.py` |
| 2026-10-30 | 4 | 数据面二进制验证：以 `c898a6c` 基线 worktree 和当前树使用相同非 LTO Release 参数构建，核心/extra 归档及 channel、fixed-channel、keyed-table 对象均逐字节相同，故本次删除未改变数据面代码或链接产物 | `cmp`：core archive 均为 `437f4a65...b8bb`，extras archive 均为 `ed63d40c...f9b5`；`git diff --quiet HEAD -- src/salias/src/channel.cpp src/salias/src/fixed_channel.cpp` exit 0；`kNamedVersion = 15` |

## 11. 本 session 门禁结论

### 已验证

- Release 构建成功，26/26 CTest 通过；ASan/UBSan 25/25 通过。删除两个兼容测试并新增 removed 契约后，
  计数与计划预期一致。
- core-only 安装不包含 extras 头、归档或 import target，并实际创建、发送和接收通用 Channel 与 typed
  `LatestMpmcChannel<4096, 75, Schema>`；extras 独立安装验证 LatestByKey 跨进程收发、Priority
  失败语义和 Ringbuffer 丢旧行为，并确认三个旧转发头未进入安装前缀。
- `mcpp build --offline --release --jobs 4`、`test_mcpp_core_import`、`test_mcpp_import` 和
  `python3 tools/check_mcpp.py` 通过；仅 `import salias;` 可独立消费，core+extras module/header
  消费覆盖 LatestByKey 与 Priority，两个独立 shared-path consumer 离线锁定构建通过。
- `python3 tools/check_channel_api.py` 通过：30 core、0 compat、9 extras、14 removed；core module
  29 个符号、extras module 9 个符号；13 个旧类型和 3 个旧头分别执行真实编译失败断言，共 16 项。
- `git diff --check` 通过，三份未跟踪/原有仓库外证据没有被本提交吸收。

### 据代码检查

- `src/salias.cppm` 不再导出四个 raw 别名，核心导出集合与 manifest 的 29 项精确一致；
  `salias/salias.hpp` 与核心 module 仍不包含 extras，旧三个转发头已从源码和安装规则删除。
- keyed table 的私有头、实现和符号只在 extras 边界。`libsalias.a` 无 `LatestByKeyStorage`，
  `libsalias_extras.a` 含 keyed factory；规范 `salias::extras` 能力未被删除。
- `Channel<Mode::LatestMpmc>::create()`/`connect()` 受 `requires` 约束关闭，wrapper 通过私有
  schema-aware 工厂继续使用同一底层 Channel 状态机。
- raw 和 typed latest 都固定容量与水位；typed 路径继续校验 schema ID、版本和 payload 大小，
  没有以类型缩减为由弱化 schema、背压、动态订阅或故障语义。
- 四个 `*Channel` 快捷别名、九个顶层 extras 兼容类型和三个转发头不再存在于当前源码；manifest
  中 13 个旧类型均为 `removed` 且五个导出面全部为 false。
- 文档主路径只展示规范入口；旧名称只出现在 removed 状态表、迁移/回滚说明和历史执行记录。
  `tools/check_channel_api.py` 扫描 `test/`、`bench/`、`example/`，规范代码无旧入口正向使用。
- 通用 Channel 与 FixedChannel 的数据面源码相对 `HEAD = c898a6c` 零差异；`kNamedVersion` 仍为 15，
  未修改共享内存 wire protocol、通道版本或固定槽热路径，未合并两个数据面，`Fast*Ring` 也未重命名。
- 用相同 `-O3 -DNDEBUG -march=native` 非 LTO 配置分别构建 `c898a6c` 基线 worktree 和当前树，
  `libsalias.a` 均为 `437f4a65...b8bb`，`libsalias_extras.a` 均为 `ed63d40c...f9b5`；
  `channel.cpp.o`、`fixed_channel.cpp.o`、`latest_by_key.cpp.o` 的 `cmp` 也全部 exit 0。该逐字节
  相同证据比数据面性能冒烟更直接，因此本阶段不重复 ABBA。

### 仍待外部确认

- 按用户决定，没有检查仓库外源码消费方，也不声称其调用量、迁移成本或私有仓库使用情况。
- 两个 Phase 0 外部证据/签署复选框继续保持未完成；仓库负责人对 Phase 4 源码删除的明确批准已单独
  记录于 manifest 和迁移文档，不把外部证据缺口伪装成已闭合。
- Phase 4 的“至少一个 released minor + release note”总门禁仍未完成：最后 tag 是 `v4.1.0`，
  Phase 3/4 变更仍位于 `[Unreleased]`。本次没有打 `5.0.0` tag，也没有更新 release note。
- Phase 5 的 TSan 全套 CTest、最终公开 API SHA-256 冻结、主版本 release note、发布签署和是否打
  tag 的决定仍未完成。下游按迁移表自行适配。
