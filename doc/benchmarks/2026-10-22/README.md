# 2026-10-22/23 Claim API v4 证据范围

本目录收录 claim API v4 三次完整正式三臂 A/B 的精简证据。第三次跨过午夜，完成于
2026-10-23 01:16。完整原始实验工作目录保留在本地 `build/`，不整目录入库。当前结论统一见
[性能报告](../../performance-report.md) §1.2 和
[迁移计划](../../plans/phase3-claim-api-v4-migration-plan.md) §4.7–4.8。

## 入库内容

| 运行 | 开始时间 | 本地完整证据 | 64 B S/B | 256 B S/B | 64 B 判定 |
|---|---|---|---:|---:|---|
| 第一次 | 2026-10-22 18:17 | `build/benchmarks/claim-api-v4-20261022-181707/` | 1.258804 | 1.281718 | GO |
| 第二次 | 2026-10-22 20:41 | `build/benchmarks/claim-api-v4-release-20261022-204139/` | 1.238393 | 1.290653 | INCONCLUSIVE |
| 第三次（独立复跑） | 2026-10-22 23:29 | `build/benchmarks/claim-api-v4-stress3-20261022-232932/` | 1.245886 | 1.320935 | INCONCLUSIVE |

每次运行入库：

- `claim-api-v4-runN-64b.md`、`claim-api-v4-runN-256b.md`：分析器生成的三段吞吐表、
  延迟中位数和轮间范围。
- `claim-api-v4-runN-analysis.json`：供机器复核的完整分析结果。
- `claim-api-v4-runN-experiment.json`：正式协议、客户端身份、块账本和运行环境。

三次运行均为 64/256 B、FIFO 1P2S、1 MiB ring、batch=1、poll-limit=64；
每格 6 组 × 20 轮 × 50M 消息。延迟为 1P1S、1 M msg/s、attempt/planned 各 10 轮 × 2 秒。
B=v3.0.0，S=v4 且 `SALIAS_DISABLE_ASYMMETRIC_FENCE=1`，A=v4 默认；S/B 单独归因
claim API，A/S 单独归因屏障，A/B 是组合效果。

第三次复跑沿用前两次冻结的 v3/v4 客户端二进制，未重新构建测试矩阵。用当前干净工作区
重新编译的 v3/v4 可执行文件与冻结客户端 `.text` 段分别逐字节相同，源码和实现哈希也一致，
因此三次口径可以直接比较。本轮运行时另有 1 秒粒度的系统负载与 cgroup 监控；36 个吞吐
block、4 个延迟 case 均退出 0，吞吐轮次共 1440 轮，cgroup `high/max/oom/oom_kill`
事件均为零。

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
| claim-api-v4-run3-64b.md | 2973 | e64e5c5a8b7aac45ace538a2c7bf3bf66943ff23b1f87bb5f0aded407240bc40 |
| claim-api-v4-run3-256b.md | 2980 | aadc0f7af9e755242ffa67bd50d27f7c66393c1e7ba82906067208219e0f67d3 |
| claim-api-v4-run3-analysis.json | 22952 | 85bfd1b0e967ee31de6bf9a71dea9361f33b88c5d4609d789db276e9be35f76b |
| claim-api-v4-run3-experiment.json | 51320 | deacc7db23d18776278efa08abe0a28745b6c2f078704bfd8c7e0c12b9269d3a |

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

python3 tools/aeron_compare/analyze_claim_v4_ab.py \
  --experiment build/benchmarks/claim-api-v4-stress3-20261022-232932 \
  --manifest build/claim-api-v4-ab-release-20261022-204034/manifest.json \
  --output /tmp/claim-v4-run3
```

分析器会重新核验块账本、客户端二进制和 runner 环境、组完整性、逐轮 RESULT 与
stdout、延迟 stdout 和 `SHA256SUMS`。本目录不含客户端二进制、完整 stdout 或组级
校验和，因此只能核对精简摘要，不能单独重算池化分位数。

第二次和第三次 64 B S/B 的预注册判定都是 **INCONCLUSIVE**：比值分别为 1.238393、
1.245886，低于 1.25，尽管各自 p=0.002165、6/6 组同向。三次独立 S/B 比值为
1.258804 / 1.238393 / 1.245886，收益方向稳定但门槛结果不稳定；不能把三次运行合并、
改用组中位数比或挑选第一次结果来宣称 GO。

第三次 64 B A/B 组合吞吐为 7.092 对 4.981 Mmsg/s（+42.37%，6/6，p=0.002165），
256 B 为 7.092 对 4.806 Mmsg/s（+47.58%，6/6，p=0.002165）。这支持在本实验拓扑、
消息大小和计数消费口径下，v4.0.0 默认路径确实比 v3.0.0 更快；不等于 claim API
单独达到 1.25 门槛。三次独立运行的点估计方向和量级一致，但组簇只有 6+6 个，
组级置换检验的最小双侧 p 为 0.002165；本次 A/B 的组簇 bootstrap 区间跨过 1，
不能把 +42%/+48% 当成高置信度的效应幅度。第三次短延迟 p50/p99 与基线接近，部分 p99.9 更低，但仍存在
轮间尖峰，且未重跑 HFT 长尾门禁。因此本目录不构成延迟非劣、生产 SLA 或 v4.0.0
整体发布验收。
