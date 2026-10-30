# Channel API 下游证据审计

日期：2026-10-30（Asia/Singapore）

## 结论

2026-10-30 对同一工作区中可检索的四个 salias 下游仓库做了源码快照审计。四个仓库都通过
`/opt/mcpp-deps/salias` 的固定绝对路径依赖 salias，并且仍直接使用四个 raw `*Channel`
别名；没有发现 `LatestByKeyChannel`、Priority 或 `Ringbuffer` 的源码引用。这提供了可复核
的本地下游引用证据，但不等同于完整的外部用户清单、迁移成本确认或负责人签署，因此 Phase 0
外部门禁仍是**阻塞**状态，而不是“零使用”。

`LatestMpmcChannel` 的 typed latest 需求同样不能由仓库内测试或这四个快照单独代表下游需求。
仓库内仍有 `Channel<Mode::LatestMpmc, Schema>` 测试，说明 typed latest 是真实存在的源码
能力；不能据此断言它必须长期保留，也不能据此删除。

## 已确认的本地下游快照

审计时间为 2026-10-30（Asia/Singapore），各仓库工作树均为干净状态：

| 下游仓库 | HEAD | salias 依赖 | 四个 raw 别名源码引用 | 文件 |
|---|---|---|---|---|
| `gts-quote` | `c0365b7557e4` | `mcpp.toml` 绝对 path | `FifoMpscChannel`、`FifoMpmcChannel` | `src/transport/ipc.cppm`、`tools/consumer/consumer.cppm` |
| `gts-signal` | `90e94171dc68` | `mcpp.toml` 绝对 path | `FifoMpscChannel`、`FifoMpmcChannel` | `src/cpp/input/ipc.cppm`、`src/cpp/output/ipc_transport.cppm`、`tests/output_tests.cpp`、`tools/reader.cppm`、`tools/signal_injector.cpp` |
| `gts-signal-gateway` | `77d74ef1d3e5` | `mcpp.toml` 绝对 path | `FifoMpscChannel`、`FifoMpmcChannel` | `src/cpp/server.cppm` |
| `gts-strategy` | `58d47889e90c` | `mcpp.toml` 绝对 path | `FifoMpscChannel`、`FifoMpmcChannel` | `src/shm/shm.cppm`、`tests/signal_feeder.cpp`、`tests/strategy_tests.cpp` |

复核命令（在 `/data/disk1/projects` 执行）：

```bash
rg -n --hidden --glob '!salias/**' --glob '!.git/**' \
  --glob '!**/build/**' --glob '!**/target/**' \
  '\b(FifoMpscChannel|FifoMpmcChannel|OrderedMpscChannel|OrderedMpmcChannel)\b' \
  gts-quote gts-signal gts-signal-gateway gts-strategy
```

四个仓库的文件与 `mcpp.toml` 依赖点在 48 条源码命中中可复核；它们都是源码兼容消费者，
因此 4.1 的 `[[deprecated]]` 不会改变运行语义、wire protocol 或要求重建现有共享通道。
但四个仓库的 `mcpp.toml` 都设置了 `-Werror`，下一次重编译会把弃用告警提升为错误；它们必须先
迁移到 `Channel<Mode>` 或显式调整告警策略，才能无错误地采用 4.1。迁移计划必须把这些仓库列入
v5 删除前门禁。

## 未闭合项

| 门禁 | 状态 | 原因 | 下一步 |
|---|---|---|---|
| 仓库外 `LatestByKey` 使用 | 阻塞 | 无可审计的外部分发/仓库依赖证据 | 收集下游仓库清单、包消费日志或 maintainer 确认 |
| 仓库外 Priority 使用 | 阻塞 | 同上；安装消费示例本身也在使用 | 同上，并确认 `test/install` 的迁移窗口 |
| 仓库外 Ringbuffer 使用 | 阻塞 | 同上 | 同上 |
| 核心保留表签署 | 阻塞 | 需要项目负责人确认 | 负责人审阅 Phase 0/1 产物并签署 |
| typed latest 归属 | 待 Phase 2 | typed 能力仍存在，wrapper 暂不覆盖 Schema | Phase 2 先定义类型安全静态配置契约 |

补充：四个已知下游仓库中未检索到上述三项 extras 的引用，但这不证明发布包或未纳入本工作区的
其他消费者为零，因此三项仍保持阻塞而不是删除依据。

## 证据规则

- 仓库内符号引用只能证明测试/工具覆盖，不能当作下游使用或零使用证明。
- 本地下游快照只能证明所列 commit 的源码引用，不能代替发布包使用遥测或用户确认。
- `LatestByKey`、Priority 和 Ringbuffer 在外部证据补齐前不得删除或标为 `removed`。
- Phase 1 的兼容迁移只移动仓库内推荐写法和文档，不改变上述三项的公开导出。
- 外部证据缺口不会阻止不破坏兼容的 Phase 1 工作，但会阻止 Phase 2-5 中任何删除或
  extras 转移决策。
