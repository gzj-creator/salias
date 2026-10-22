# 性能历史记录

本目录保存历史实验报告、失败候选和不同口径的原始说明，不是当前优化交接入口。

当前唯一汇总入口是 [最终性能测试与优化交接报告](../../performance-report.md)。
阅读该报告即可了解当前产品、Salias/Aeron 同组对比、证据边界和下一轮优化顺序；
需要追溯某项实验时再阅读本目录。历史数字不能跨版本、拓扑或计时口径拼接。

## 历史索引

| 日期 | 报告 | 用途与限制 |
|---|---|---|
| 2026-10-19 | [设计诊断](aeron-design-diagnosis-2026-10-19.md) | 真实 profile、分段计时与调度关联，非完整硬件 PMU 归因 |
| 2026-10-19 | [安全 poll 优化确认](aeron-design-implementation-2026-10-19.md) | 调用独占描述符存储、调度反例与不安全协议反例 |
| 2026-10-19 | [轮询后续候选](aeron-poll-path-followup-2026-10-19.md) | 六候选未晋升，保留全部格子和不利结果 |
| 2026-10-19 | [较早 1P1S Aeron 对比](aeron-latency-comparison-2026-10-19.md) | 不与新 1P2S 结果拼接 |
| 2026-10-18 | [阶段总结](performance-summary-2026-10-18.md) | 当日 HFT/Publisher 结论，不是当前入口 |
| 2026-10-18 | [合成行情长测](hft-market-data-acceptance-2026-10-18.md) | 30 分钟、八配置，50 us p99.9 门禁未通过 |
| 2026-10-18 | [早期 Publisher](publisher-latency-2026-10-18.md) | 首次尝试计时，较短轮次 |
| 2026-10-18 | [Publisher 相对收益](publisher-tail-relative-acceptance-2026-10-18.md) | 三组 ABBA 失败，新候选撤销 |
| 2026-10-18 | [尾延迟实现说明](tail-latency-optimization-2026-10-18.md) | 历史实现阶段设计，不等于实测通过 |
| 2026-10-18 | [尾延迟独立验收](tail-latency-acceptance-2026-10-18.md) | A/B 与长测结果，包含未完成输入 |
| 2026-09-25 | [批量接收优化](performance-optimization-2026-09-25.md) | 历史内核微基准，不替代进程间端到端结论 |
| 2026-07-11 | [旧吞吐报告](throughput-2026-07-11.md) / [24 配置表](throughput-table-2026-07-11.md) | 旧版本、2P1S/2P2S，与当前 1P2S 不同 |

归档仅补历史标识和修复相对链接，保留原始实验结论；机器可读数据仍在
`doc/benchmarks/`。旧设计实验及 Publisher ABBA 完整本地产物已无损压缩至
`build/benchmark-cleanup-20261019/`，归档内保留原目录结构；历史绝对路径需恢复后使用。
位置、哈希清单与恢复方法见[最终报告第 8.2 节](../../performance-report.md)。
