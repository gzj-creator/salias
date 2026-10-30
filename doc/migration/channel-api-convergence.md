# Channel API 收敛迁移说明

日期：2026-10-30（Asia/Singapore）

## 1. 兼容窗口

本次是源码 API 收敛，不修改共享内存 wire protocol，也不要求升级通道控制段版本。旧别名和规范入口
是同一类型的同义词，迁移不改变排序、背压、schema、动态订阅、TTL、Latest 或故障语义。

四个 raw `*Channel` 别名从 `4.1.0` 起：

- 继续由 `<salias/salias.hpp>` 和 `import salias;` 导出；
- 标记 `[[deprecated]]`，使用点产生编译器弃用警告；
- 只保留在兼容测试和迁移说明中，不再出现在 README 快速开始、示例或 benchmark 主路径；
- 最早在 `5.0.0` 删除，且删除前必须通过下游证据、安装消费、module 消费和负责人签署门禁。

本版本不删除任何别名，不执行主版本 tag，也不处理 `Fast*Ring` 最终重命名。

## 2. 源码迁移表

| 兼容入口 | 规范入口 | 迁移命令 | 备注 |
|---|---|---|---|
| `salias::FifoMpscChannel` | `salias::Channel<salias::Mode::FifoMpsc>` | `perl -pi -e 's/\\bsalias::FifoMpscChannel\\b/salias::Channel<salias::Mode::FifoMpsc>/g' FILES` | 类型完全同型 |
| `salias::FifoMpmcChannel` | `salias::Channel<salias::Mode::FifoMpmc>` | `perl -pi -e 's/\\bsalias::FifoMpmcChannel\\b/salias::Channel<salias::Mode::FifoMpmc>/g' FILES` | fanout 语义不变 |
| `salias::OrderedMpscChannel` | `salias::Channel<salias::Mode::OrderedMpsc>` | `perl -pi -e 's/\\bsalias::OrderedMpscChannel\\b/salias::Channel<salias::Mode::OrderedMpsc>/g' FILES` | 全局顺序语义不变 |
| `salias::OrderedMpmcChannel` | `salias::Channel<salias::Mode::OrderedMpmc>` | `perl -pi -e 's/\\bsalias::OrderedMpmcChannel\\b/salias::Channel<salias::Mode::OrderedMpmc>/g' FILES` | 全局顺序与 fanout 语义不变 |
| `salias::Channel<salias::Mode::LatestMpmc>`（raw） | `salias::LatestMpmcChannel<Capacity, HighWatermarkPercent>` | 手工迁移，先固定容量和水位模板参数 | `connect()` 会校验静态配置 |
| `salias::Channel<salias::Mode::LatestMpmc, Schema>`（typed） | **待 Phase 2 决策** | 不得自动替换为 raw wrapper | wrapper 当前无 Schema 参数，不能弱化类型/版本校验 |

`LatestMpmcPublisher`、`LatestMpmcSubscriber` 和其他模式端点的显式别名是端点拼写，不是 Channel
产品选择入口；本版本没有删除计划。新代码可统一写 `Publisher<Mode, Schema>` /
`Subscriber<Mode, Schema>`，但无需仅为书写形式改动已编译代码。

## 3. 推荐替换策略

1. 先替换仓库内 primary path，不把兼容测试、迁移说明和 release note 作为待迁移代码。
2. 使用编译器的弃用警告定位下游残留；`-Werror` 消费者应一次性完成 codemod，而不是关闭警告。
3. 删除注释、README、示例或 benchmark 中的旧名称时，保留迁移表和兼容测试，以证明窗口仍开放。
4. typed latest 在 Phase 2 补齐静态配置和 schema 契约前保持原样；不得为了消除 compat 入口而
   把 typed 通道降级为 raw payload。

## 4. 仓库内 codemod 示例

先列出命中，再限定文件执行：

```bash
rg -n '\\bsalias::(FifoMpsc|FifoMpmc|OrderedMpsc|OrderedMpmc)Channel\\b' \
  test bench example test/install test/mcpp
```

本次已对 `example/`、`bench/` 和非兼容专项测试迁移；兼容覆盖由
`test/api/channel_alias_compat_test.cpp` 独立保留。迁移后的主代码不再使用旧别名。

同一工作区可检索的四个下游仓库（`gts-quote`、`gts-signal`、`gts-signal-gateway`、
`gts-strategy`）仍使用 `FifoMpscChannel` / `FifoMpmcChannel`。它们不在本仓库迁移范围内，
但必须在 v5 删除前完成 codemod 并重编译；清单和 commit 快照见
[下游证据审计](../api/channel-api-downstream-evidence.md)。

## 5. 删除门禁

- 至少一个 minor 的公开弃用窗口；
- 下游使用清单、安装消费和 module 消费证据闭环；
- `LatestByKey`、Priority、Ringbuffer 的仓库外证据与负责人签署完成；
- typed latest 的唯一入口决策完成；
- release note 给出 codemod 和回滚说明；
- 只允许在 `5.0.0` 主版本删除，不借 patch/minor 静默破坏源码兼容。

当前阻塞项见 [下游证据审计](../api/channel-api-downstream-evidence.md)，完整 symbol 状态见
[公开 API 清单](../api/channel-api-manifest.json)。
