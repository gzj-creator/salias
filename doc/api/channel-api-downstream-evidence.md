# Channel API 下游证据审计

日期：2026-10-30（Asia/Singapore）

## 结论

本仓库无法自行证明仓库外是否存在 `LatestByKeyChannel`、Priority 或 `Ringbuffer`
消费者。当前没有消费者清单、依赖图、发布遥测、兼容性问题记录或用户确认可供审计，
因此这三项的移除/迁移成本与 Phase 0 外部使用门禁仍是**阻塞**状态，而不是“零使用”。

`LatestMpmcChannel` 的 typed latest 需求同样不能由仓库内测试单独代表下游需求。仓库内仍有
`Channel<Mode::LatestMpmc, Schema>` 测试，说明 typed latest 是真实存在的源码能力，但
不能据此断言它必须长期保留，也不能据此删除。

## 未闭合项

| 门禁 | 状态 | 原因 | 下一步 |
|---|---|---|---|
| 仓库外 `LatestByKey` 使用 | 阻塞 | 无可审计的外部分发/仓库依赖证据 | 收集下游仓库清单、包消费日志或 maintainer 确认 |
| 仓库外 Priority 使用 | 阻塞 | 同上；安装消费示例本身也在使用 | 同上，并确认 `test/install` 的迁移窗口 |
| 仓库外 Ringbuffer 使用 | 阻塞 | 同上 | 同上 |
| 核心保留表签署 | 阻塞 | 需要项目负责人确认 | 负责人审阅 Phase 0/1 产物并签署 |
| typed latest 归属 | 待 Phase 2 | typed 能力仍存在，wrapper 暂不覆盖 Schema | Phase 2 先定义类型安全静态配置契约 |

## 证据规则

- 仓库内符号引用只能证明测试/工具覆盖，不能当作下游使用或零使用证明。
- `LatestByKey`、Priority 和 Ringbuffer 在外部证据补齐前不得删除或标为 `removed`。
- Phase 1 的兼容迁移只移动仓库内推荐写法和文档，不改变上述三项的公开导出。
- 外部证据缺口不会阻止不破坏兼容的 Phase 1 工作，但会阻止 Phase 2-5 中任何删除或
  extras 转移决策。
