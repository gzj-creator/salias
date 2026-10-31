# Channel API 收敛迁移说明

日期：2026-10-31（Asia/Shanghai）

## 1. 当前状态

本文件描述 `v6.0.0` 的迁移边界。`v5.0.0` 已删除四个 raw `*Channel` 快捷别名、三个旧 extras
转发头和九个顶层 extras 别名；`v6.0.0` 进一步永久删除 `salias::extras` 的全部规范能力、headers、
CMake target/component 和 module。

本次删除不修改共享内存 wire protocol，不升级通道控制段版本，不合并 `Channel` 与 `FixedChannel`
数据面。因为删除的是源码、打包和 module 入口，迁移要求重新编译；仅因 API 清理不要求重建已有共享
通道。不存在仍导出的 Channel 产品兼容别名或 extras 兼容入口。

`LatestByKey`、Priority 和 Ringbuffer 没有替代 API。应用必须自行实现对应组件、改用其他库，或重新
设计业务方案；本库不把 `Channel`、`FixedChannel` 或 `LatestMpmcChannel` 宣称为这些能力的一对一
替代品。删除项在 [公开 API 清单](../api/channel-api-manifest.json)中状态为 `removed`，反向契约由
`test/api/removed_api_contract_test.cpp`、`test/mcpp/import.cpp`、安装消费测试和
`tools/check_channel_api.py` 锁定。

## 2. 迁移表

### 2.1 通用 Channel 快捷别名（v5.0.0）

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

### 2.2 extras 规范入口（v6.0.0 永久删除）

以下九个规范类型没有替代入口。v6.0.0 不再提供弃用窗口，也不保留 namespace、header、CMake target
或 module：

| 已删除入口 | 替代入口 |
|---|---|
| `salias::extras::LatestByKeyChannel` | 无 |
| `salias::extras::LatestByKeyPublisher` | 无 |
| `salias::extras::LatestByKeySubscriber` | 无 |
| `salias::extras::LatestByKeyMessage` | 无 |
| `salias::extras::PriorityPublisher` | 无 |
| `salias::extras::PrioritySubscriber` | 无 |
| `salias::extras::PriorityMessage` | 无 |
| `salias::extras::PriorityWaitPolicy` | 无 |
| `salias::extras::Ringbuffer` | 无 |

同时删除的 header 和打包/module 边界：

| 已删除入口 | 替代入口 |
|---|---|
| `<salias/extras/extras.hpp>` | 无 |
| `<salias/extras/latest_by_key_channel.hpp>` | 无 |
| `<salias/extras/priority_channel.hpp>` | 无 |
| `<salias/extras/ringbuffer.hpp>` | 无 |
| `salias::extras` CMake target/component | 无 |
| `import salias.extras;` | 无 |

`v5.0.0` 已删除的旧顶层 extras 名称和旧转发头也继续保持 removed：

| 已删除入口 | 替代入口 |
|---|---|
| `<salias/latest_by_key_channel.hpp>` | 无 |
| `<salias/priority_channel.hpp>` | 无 |
| `<salias/ringbuffer.hpp>` | 无 |
| `salias::LatestByKeyChannel` | 无 |
| `salias::LatestByKeyPublisher` | 无 |
| `salias::LatestByKeySubscriber` | 无 |
| `salias::LatestByKeyMessage` | 无 |
| `salias::PriorityPublisher` | 无 |
| `salias::PrioritySubscriber` | 无 |
| `salias::PriorityMessage` | 无 |
| `salias::PriorityWaitPolicy` | 无 |
| `salias::Ringbuffer` | 无 |

迁移时必须删除 namespace、include、链接和 module 引用，不能仅做文本替换。`Error::CapacityExceeded`
和 `Error::SequenceExhausted` 名称也随 LatestByKey 删除；枚举数值 10/11 保持空洞，剩余错误码数值
不变。应用应自行定义所需的错误与同步策略，不要把核心 Channel 的背压语义当作 extras 的兼容替代。

### 2.3 直接 latest 工厂（v5.0.0）

直接 `Channel<salias::Mode::LatestMpmc>` 的 `create()` / `connect()` 已关闭。raw 和 typed latest
都必须使用 `LatestMpmcChannel`：

| 已删除路径 | 规范入口 |
|---|---|
| `salias::Channel<salias::Mode::LatestMpmc>` 的直接创建/连接 | `salias::LatestMpmcChannel<Capacity, HighWatermarkPercent>` |
| `salias::Channel<salias::Mode::LatestMpmc, Schema>` 的直接创建/连接 | `salias::LatestMpmcChannel<Capacity, HighWatermarkPercent, Schema>` |

typed 用户必须保留第三个 `Schema` 模板参数。不能为了减少类型而改为 raw payload；该 wrapper
继续校验 schema ID、版本、payload 大小、固定容量和水位，并保留背压、动态订阅和故障语义。

## 3. 版本、弃用与回滚策略

- `v4.1.0` 是旧快捷入口最后一个带弃用提示的已发布 tag。
- `v5.0.0` 删除四个 raw 别名、九个顶层 extras 名称和三个旧转发头。
- `v6.0.0` 是新的 breaking release，永久删除九个规范 extras 类型及其 header、target/component、
  module、测试和 benchmark。该决定由仓库负责人于 2026-10-31 确认，以维护成本为主要依据，不依赖
  未采集的仓库外使用数据。
- 不提供 v6 弃用窗口。需要这些能力的应用必须停留在 `v5.0.0` 或自行迁移；回滚整个发布可回到
  `v5.0.0`，但 wire protocol 和通道版本在 v5/v6 间未变，回滚 API/打包层不要求修改已有共享内存布局。

## 4. 仓库外证据边界

按负责人决定，没有扫描或下载仓库外源码消费方，也不声称其调用量、迁移成本或私有仓库零使用。
`doc/api/channel-api-downstream-evidence.md`、`channel-api-phase0-evidence.json` 和签署材料仍属于
仓库外证据缺口并标记为阻塞，不能用本次仓库内验证替代。

## 5. 验证与剩余门禁

本次 v6 发布已由以下仓库内门禁验证：

- Release CTest 23/23、ASan/UBSan CTest 22/22、TSan CTest 22/22 覆盖通用 Channel、
  `FixedChannel`、Latest、版本一致性、安装消费和故障语义。
- `test/api/removed_api_contract_test.cpp` 用 `__has_include` 锁定 extras umbrella、三个专项头、
  三个旧转发头和旧 detail header 均不可见。
- `tools/check_channel_api.py` 对旧快捷入口、旧 extras 名称和 `salias::extras::*` 规范名称执行逐个
  真实编译失败断言，并检查 extras header/module/target 均不存在。
- `test/mcpp/core_import.cpp` 与 `test/mcpp/import.cpp` 只消费 `import salias;`；header probe 不再
  包含 extras；安装消费测试只安装 `core` component。
- `tools/check_channel_api.py` 校验 manifest、core module、umbrella、安装面、文档和反向契约一致。
- API 冻结基线绑定 `v6.0.0`，覆盖删除后的公开/安装文件和聚合 SHA-256，说明见
  [Channel API 冻结基线](../api/channel-api-freeze.md)。

删除前后的影响已以“热路径未修改 + 二进制符号差异 + 短时 channel benchmark”验证：核心归档
symbol 集合一致，两套 benchmark 可执行文件的代码/数据段一致，104M 条消息全量校验通过；由于两臂
代码相同，不据此宣称性能改善或退化。执行记录见
[v6 维护收敛计划](../plans/2026-10-31-salias-v6-extras-removal.md)。Phase 0 未采集的仓库外使用证据
仍是独立审计缺口，不能被解释为全局零使用。
