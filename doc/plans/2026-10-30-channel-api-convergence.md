# Salias Channel API 收敛计划

日期：2026-10-30（Asia/Singapore）

状态：**Phase 1 规范入口与文档冻结完成；Phase 0 已采集本地下游快照但完整外部证据和负责人签署仍阻塞；Phase 2-5 未执行**

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
| 直接 latest 模式 | `Channel<Mode::LatestMpmc>` | 与上一行同引擎，但绕过静态配置门禁 | **保留兼容，停止主推** |
| 按 key 最新值 | `LatestByKeyChannel<Key, Value, Capacity, Version>` | 进程共享锁、key 表、快照和事件日志 | **extras 候选** |
| 优先级组合 | `PriorityPublisher<Mode...>` / `PrioritySubscriber<Mode...>` | 多通道路由和加权轮询 | **extras 候选** |
| 本地队列 | `Ringbuffer<T, Capacity, Watermark>` | 进程内单线程有界丢旧队列 | **extras 候选** |

`Publisher`、`Subscriber`、`PublishClaim`、`LatestByKeyMessage` 等端点类型由通道返回，不是用户需要
独立选型的产品；文档应继续保留其契约，但不把它们与 Channel 产品并列宣传。

### 3.2 当前复杂度证据

- 通用引擎公开 5 个 `Mode`：`FifoMpsc`、`FifoMpmc`、`OrderedMpsc`、`OrderedMpmc`、
  `LatestMpmc`。
- `FixedChannel` 公开 4 个 topology：`Spsc`、`Mpsc`、`Spmc`、`Mpmc`。
- 同时存在 4 个 raw `*Channel` 别名和 `Channel<Mode>` 规范形式。
- `LatestMpmc` 同时存在 wrapper 和直接 `Channel<Mode::LatestMpmc>` 两条公开路径。
- `LatestByKeyChannel` 的公开头与实现约 1,220 行，专项测试约 1,067 行。
- Priority 的公开头约 511 行，专项测试约 564 行。
- 统一伞头文件 `salias/salias.hpp` 当前同时导出核心通道、Latest、LatestByKey、Priority 和 Ringbuffer。

### 3.3 现有决策冲突

当前仓库没有正式的 API 淘汰计划。Fast Ring 计划明确要求通用 `Channel` 继续保留 Ordered、Latest、
TTL、动态订阅和恢复语义，见
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
using Quotes = salias::LatestMpmcChannel<1u << 20, 90>;
```

`LatestMpmcChannel` 保持为推荐主入口，因为它把容量和水位放在模板中，并在 `connect()` 时校验配置。
直接 `Channel<Mode::LatestMpmc>` 暂保留兼容，但不再作为 README 和示例的推荐形式；是否在主版本中
移出公开面，取决于 typed latest 是否成为正式需求。

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
7. **删除必须有证据。** 至少完成仓库引用清零、安装消费、模块消费、专项测试和 release note。

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

本 session 已对仓库内 `test/`、`bench/`、`example/`、安装消费、mcpp 和 module 导出完成
引用审计，结果见 [仓库内使用矩阵](../api/channel-api-usage-matrix.md)。另对同一工作区可检索的
四个 salias 下游仓库采集了源码快照：均使用固定绝对路径依赖并引用四个 raw 别名，未检索到
`LatestByKey`、Priority 或 Ringbuffer 源码引用。由于仍缺发布包消费遥测、完整外部用户清单和
负责人签署，两个 Phase 0 外部/签署复选框保持未勾选；证据与边界见
[下游证据审计](../api/channel-api-downstream-evidence.md)，不以仓库内测试代替下游证据。

### Phase 1：规范入口与文档冻结

- [x] 新增公开 API 清单，按 core、compat、extras 三个状态记录每个 symbol。
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
因此不得据此进入任何删除或 extras 转移步骤。

### Phase 2：Latest 主入口收敛

- [ ] 确认 `LatestMpmcChannel` 是公开 latest fanout 的唯一推荐入口。
- [ ] 盘点直接使用 `Channel<Mode::LatestMpmc>` 的测试、benchmark、模块导出和安装消费。
- [ ] 决定 typed latest 是否属于核心需求。
- [ ] 若需要 typed latest，先提供与 `LatestMpmcChannel` 一致的类型安全和静态配置契约，不暴露第三条路径。
- [ ] 将直接 `Channel<Mode::LatestMpmc>` 标记为 compat-only，并停止在文档中并列推荐。
- [ ] 增加测试，确保 wrapper 与底层模式在容量、水位、策略和连接校验上完全一致。
- [ ] 在主版本前保留底层模式，避免同版本删除造成兼容断裂。

**Phase 2 退出条件**：Latest 只有一个推荐入口，且不存在静态配置校验旁路。

### Phase 3：extras 边界

- [ ] 设计 `salias::extras` 命名空间、include 根目录和可选 CMake target。
- [ ] 将 `LatestByKeyChannel`、`LatestByKeyPublisher`、`LatestByKeySubscriber` 和
  `LatestByKeyMessage` 移入 extras。
- [ ] 将 `PriorityPublisher`、`PrioritySubscriber`、`PriorityMessage` 和
  `PriorityWaitPolicy` 移入 extras。
- [ ] 将 `Ringbuffer` 移入 extras，或作为独立的 header-only utility target 发布。
- [ ] 将 keyed table 实现从核心 `salias` library 拆出，确保未使用 extras 的消费者不链接相关实现。
- [ ] 更新 `mcpp.toml`、`src/salias.cppm`、安装规则、package config 和 header probes。
- [ ] 增加“只链接 core 不包含 extras”的安装消费测试。
- [ ] 增加“extras 单独安装和跨进程运行”测试。
- [ ] 更新 README、API 文档、示例、benchmark 和性能报告中的 include/link 路径。
- [ ] 提供旧 include 到新 include 的迁移表和一版兼容转发头。

**Phase 3 退出条件**：core 构建不依赖 extras 实现；extras 可独立安装、链接、模块导入并通过测试。

### Phase 4：兼容别名与重复入口删除

- [ ] 在弃用窗口至少一个 minor 后，确认构建警告、release note 和迁移示例已发布。
- [ ] 删除 `FifoMpscChannel`、`FifoMpmcChannel`、`OrderedMpscChannel` 和
  `OrderedMpmcChannel` 别名，或由负责人明确批准永久保留并更新本计划。
- [ ] 按 Phase 2 决策删除或隐藏直接 `Channel<Mode::LatestMpmc>` 公开路径。
- [ ] 删除 extras 转发头；兼容窗口结束前不得提前删除。
- [ ] 将 `salias/salias.hpp` 收敛为核心聚合头，不再默认包含 extras。
- [ ] 更新 C++23 module export，移除核心 module 中的 extras 符号。
- [ ] 清理文档中的重复入口、废弃命名和双份示例。
- [ ] 确认没有内部代码通过 compat 别名遮蔽规范路径。

**Phase 4 退出条件**：核心公开面中不存在同语义双入口；所有删除项在下游/模块/安装测试中清零。

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
| 当前 `4.x` minor | 增加弃用标记、规范文档、兼容转发头、extras 试验 target；不删除公开符号 |
| 下一个 `5.0.0` | 删除兼容别名、拆分 extras target、收敛伞头文件和 module export；要求重新编译 |
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
- 旧 include/别名在弃用窗口内仍可编译，并产生明确迁移警告。
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

| 日期 | Phase | 结果 | 证据 |
|---|---|---|---|
| 2026-10-30 | 0 | 计划落盘；确认当前无正式 API 收敛计划 | 本文件 |
| 2026-10-30 | 0 | 完成公开入口盘点：5 个 Mode、4 个 FixedTopology、4 个 raw 别名、2 条 Latest 路径和 3 个 extras 候选 | `src/salias/include/salias/{config.hpp,fixed_channel.hpp,channel.hpp,latest_channel.hpp,latest_by_key_channel.hpp,priority_channel.hpp,ringbuffer.hpp}` |
| 2026-10-30 | 0 | 记录外围规模：LatestByKey 实现约 1,220 行、专项测试约 1,067 行；Priority 实现约 511 行、专项测试约 564 行 | `wc -l src/salias/include/salias/latest_by_key_channel.hpp src/salias/src/latest_by_key.cpp src/salias/include/salias/priority_channel.hpp test/api/latest_by_key_test.cpp test/api/priority_channel_test.cpp` |
| 2026-10-30 | 0 | 完成 test/bench/example/install/mcpp 引用矩阵；确认安装消费仍直接依赖 Priority，mcpp 仍聚合导出 extras 候选，留待 Phase 3 | `doc/api/channel-api-usage-matrix.md`；`test/install/main.cpp`、`test/mcpp/{header_probe,import}.cpp`、`src/salias.cppm` |
| 2026-10-30 | 0 | 采集同工作区四个 salias 下游仓库源码快照：`gts-quote`、`gts-signal`、`gts-signal-gateway`、`gts-strategy` 均在 11 个文件中共 48 行引用 `FifoMpscChannel`/`FifoMpmcChannel`；未检索到 `LatestByKey`、Priority 或 Ringbuffer 引用，但不能据此断言全局零使用 | `/data/disk1/projects/{gts-quote,gts-signal,gts-signal-gateway,gts-strategy}`；commit 快照与复核命令见 `doc/api/channel-api-downstream-evidence.md` |
| 2026-10-30 | 0 | 四个下游仓库的 `mcpp.toml` 均启用 `-Werror`；代表性 `FifoMpscChannel` 消费在同等告警配置下因 `-Werror=deprecated-declarations` 编译失败，确认迁移是采用 4.1 的前置条件，不是零成本重编译 | `rg -n 'cxxflags|Werror' /data/disk1/projects/{gts-quote,gts-signal,gts-signal-gateway,gts-strategy}/mcpp.toml`；代表性 `c++ -std=c++23 ... -Werror -fsyntax-only`，exit 1 |
| 2026-10-30 | 0 | 仓库外完整用户清单、发布包消费遥测和迁移成本确认仍不可获得；负责人签署待办，Phase 0 两个对应复选框保持未完成 | `doc/api/channel-api-downstream-evidence.md` |
| 2026-10-30 | 1 | 建立机器可读公开 API 清单：30 core、5 compat、9 extras、0 removed；每项含代码位置、状态、替代入口、header/module/install/mcpp 导出状态 | `doc/api/channel-api-manifest.json`；`python3 tools/check_channel_api.py` 输出 `Channel API manifest OK` |
| 2026-10-30 | 1 | README 快速开始冻结为 `Channel<Mode, Schema>`、`FixedChannel`、`LatestMpmcChannel` 三个核心入口；四个别名与直接 latest 模式移入兼容说明；raw/typed 明示为同一 Channel 的 payload 模式 | `README.md`、`doc/11-L7-api.md` |
| 2026-10-30 | 1 | 示例、benchmark、mcpp 和非兼容专项测试迁移到 `Channel<Mode>`；四别名只保留在 `channel_alias_compat_test.cpp`；别名标 `[[deprecated]]`，本版本不删除 | `rg -n 'FifoMpscChannel|FifoMpmcChannel|OrderedMpscChannel|OrderedMpmcChannel' test bench example test/mcpp`；兼容测试、`doc/migration/channel-api-convergence.md` |
| 2026-10-30 | 1 | 评估并采用 `Schema<T, Id, Version>`：静态断言非零显式 ID/version，空对象、无运行时成本；typed Channel 与 mcpp 头/模块消费均覆盖 | `src/salias/include/salias/message.hpp`、`test/api/schema_helper_test.cpp`、`test/mcpp/typed_schema.hpp` |
| 2026-10-30 | 1 | 新增文档门禁：校验 symbol 状态唯一性、代码位置、module/cpp 导出一致性、四别名兼容文档、README 快速开始禁入项与版本一致性 | `tools/check_channel_api.py`、`test/CMakeLists.txt` 的 `salias_channel_api_documentation_check` |
| 2026-10-30 | 1 | Release 构建与 25/25 CTest 通过，其中安装消费、文档检查、别名兼容和 Schema helper 用例均通过；ASan/UBSan 24/24 通过；mcpp 模块/头文件混合消费通过 | `cmake --build --preset release -j 4`；`ctest --test-dir build/release --output-on-failure -j 2`；`cmake --build --preset debug-asan-ubsan -j 4`；`ctest --preset debug-asan-ubsan --output-on-failure -j 2`；`mcpp build --offline --release --jobs 4`；`mcpp run test_mcpp_import --offline --release --locked --features mcpp-tests`；`python3 tools/check_mcpp.py`；`git diff --check` |

## 11. 本 session 门禁结论

已完成：Phase 1 的规范入口、兼容标记、迁移说明、机器可读清单、仓库内迁移、文档检查、Release
构建、安装消费和 mcpp header/import 验证。

未完成：Phase 0 的完整仓库外用户清单、包消费遥测、迁移成本确认和负责人签署；当前已采集
四个本地下游源码快照，但不能用它们替代完整外部证据或签署。两项 Phase 0 复选框保持未完成。
Phase 2-5 按本次 session 目标未执行；四个 `*Channel` 别名、`LatestByKey`、Priority、Ringbuffer
和直接 latest 模式均保留。本阶段没有修改共享内存 wire protocol、通道版本、固定槽热路径或
`Fast*Ring` 名称，因此没有执行性能 ABBA；Release 二进制仅包含模板/文档/测试层变化。

下一步门禁：先收集下游使用证据并完成负责人签署；随后 Phase 2 才能处理 typed latest 和直接
`Channel<Mode::LatestMpmc>` 的兼容边界，Phase 3 才能规划 extras target。任何删除前都必须重新运行
安装、module、兼容性和数据面性能门禁。
