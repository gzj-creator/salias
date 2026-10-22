# 2026-10-22 Claim API v4 证据范围

本目录收录 claim API v4 两次完整正式三臂 A/B 的精简证据。完整 12 MiB、2648 个文件的
实验工作目录保留在本地 `build/`，不整目录入库。当前结论统一见
[性能报告](../../performance-report.md) §1.2 和
[迁移计划](../../plans/phase3-claim-api-v4-migration-plan.md) §4.7–4.8。

## 入库内容

| 运行 | 开始时间 | 本地完整证据 | 64 B S/B | 256 B S/B | 64 B 判定 |
|---|---|---|---:|---:|---|
| 第一次 | 2026-10-22 18:17 | `build/benchmarks/claim-api-v4-20261022-181707/` | 1.258804 | 1.281718 | GO |
| 第二次 | 2026-10-22 20:41 | `build/benchmarks/claim-api-v4-release-20261022-204139/` | 1.238393 | 1.290653 | INCONCLUSIVE |

每次运行入库：

- `claim-api-v4-runN-64b.md`、`claim-api-v4-runN-256b.md`：分析器生成的三段吞吐表、
  延迟中位数和轮间范围。
- `claim-api-v4-runN-analysis.json`：供机器复核的完整分析结果。
- `claim-api-v4-runN-experiment.json`：正式协议、客户端身份、块账本和运行环境。

两次运行均为 64/256 B、FIFO 1P2S、1 MiB ring、batch=1、poll-limit=64；
每格 6 组 × 20 轮 × 50M 消息。延迟为 1P1S、1 M msg/s、attempt/planned 各 10 轮 × 2 秒。
B=v3.0.0，S=v4 且 `SALIAS_DISABLE_ASYMMETRIC_FENCE=1`，A=v4 默认；S/B 单独归因
claim API，A/S 单独归因屏障，A/B 是组合效果。

## 文件校验

| 文件 | 字节 | SHA-256 |
|---|---:|---|
| claim-api-v4-run1-64b.md | 2965 | 867a09a6309720ba786184a5949b501aaaace36c3741700dc4d5899f02db73a9 |
| claim-api-v4-run1-256b.md | 2986 | 810edf506c7ef3e5861087c81ad019d6b5de04f7a9210b0da98f6777d01f823f |
| claim-api-v4-run1-analysis.json | 23044 | 44639b942cdaf26257a126432b547ab679090a7b253149f6fdae6ff4c09eca85 |
| claim-api-v4-run1-experiment.json | 49172 | ab5de17b0be06027f42617bdc76714475a3ad2926141e6e09e6174cb586adf48 |
| claim-api-v4-run2-64b.md | 2983 | 81401d16594a205621fa24687ad9dc27c76da514c18ba0df5d72b75e8c7db466 |
| claim-api-v4-run2-256b.md | 2994 | ec6bd9cd150779e18993e25c5c2b5db0670ed5ad7a178a36089e0166ee0e2de0 |
| claim-api-v4-run2-analysis.json | 22954 | 087d5ae403836b84bbf6a95af89551c0b660e31e62af37e6027d414605232f42 |
| claim-api-v4-run2-experiment.json | 51315 | 67a397d4da7615e89d280339bd0fe22aa9ea2707e5c9fce7a13df8ee451718fe |

## 重算与边界

```bash
python3 tools/aeron_compare/analyze_claim_v4_ab.py \
  --experiment build/benchmarks/claim-api-v4-20261022-181707 \
  --manifest build/claim-api-v4-ab-v3/manifest.json \
  --output /tmp/claim-v4-run1

python3 tools/aeron_compare/analyze_claim_v4_ab.py \
  --experiment build/benchmarks/claim-api-v4-release-20261022-204139 \
  --manifest build/claim-api-v4-ab-release-20261022-204034/manifest.json \
  --output /tmp/claim-v4-run2
```

分析器会重新核验块账本、客户端二进制和 runner 环境、组完整性、逐轮 RESULT 与
stdout、延迟 stdout 和 `SHA256SUMS`。本目录不含客户端二进制、完整 stdout 或组级
校验和，因此只能核对精简摘要，不能单独重算池化分位数。

第二次 64 B S/B 的预注册判定是 **INCONCLUSIVE**：比值 1.238393 低于 1.25，尽管
p=0.002165、6/6 组同向。不能把两次运行合并、改用组中位数比或挑选第一次结果来
宣称 GO。延迟短测仍有毫秒级尖峰，未重跑 HFT 长尾门禁，因此本目录不构成延迟非劣、
生产 SLA 或 v4.0.0 整体发布验收。
