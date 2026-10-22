# Claim API v4 三臂 A/B 分析

负载：64 B，FIFO 1P2S，1 MiB，batch=1，poll-limit=64。
B=v3，S=v4+顺序屏障，A=v4+默认非对称屏障。
延迟为各臂 10 轮独立分位数的中位数，不做直方图合并。

## 吞吐

| 对比 | 候选 Mmsg/s | 基线 Mmsg/s | 相对变化 | 组方向 | 精确置换 p |
|---|---:|---:|---:|---:|---:|
| claim_api_S_vs_B | 6.145 | 4.882 | +25.88% | 6/6 | 0.002165 |
| fence_A_vs_S | 7.048 | 6.145 | +14.68% | 6/6 | 0.002165 |
| combined_A_vs_B | 7.048 | 4.882 | +44.36% | 6/6 | 0.002165 |

claim API 64 B 预注册吞吐判定：**GO**

## 延迟

| 对比 | 口径 | p50 候选/基线 (ns) | p99 候选/基线 (ns) | p99.9 候选/基线 (ns) |
|---|---|---:|---:|---:|
| claim_api_S_vs_B | attempt | 385.0/379.0 (1.016x) | 479.0/491.0 (0.976x) | 4447.0/4383.0 (1.015x) |
| claim_api_S_vs_B | planned | 441.0/447.0 (0.987x) | 4959.0/4895.0 (1.013x) | 13247.0/13695.0 (0.967x) |
| fence_A_vs_S | attempt | 385.0/385.0 (1.000x) | 481.0/479.0 (1.004x) | 4143.0/4447.0 (0.932x) |
| fence_A_vs_S | planned | 441.0/441.0 (1.000x) | 4223.0/4959.0 (0.852x) | 11839.0/13247.0 (0.894x) |
| combined_A_vs_B | attempt | 385.0/379.0 (1.016x) | 481.0/491.0 (0.980x) | 4143.0/4383.0 (0.945x) |
| combined_A_vs_B | planned | 441.0/447.0 (0.987x) | 4223.0/4895.0 (0.863x) | 11839.0/13695.0 (0.864x) |

### 延迟轮间范围

| 对比 | 口径 | 指标 | 候选 min–max (ns) | 基线 min–max (ns) |
|---|---|---|---:|---:|
| claim_api_S_vs_B | attempt | p50 | 375–427 | 359–399 |
| claim_api_S_vs_B | attempt | p99 | 447–3423 | 467–3519 |
| claim_api_S_vs_B | attempt | p999 | 3743–13567 | 3871–14463 |
| claim_api_S_vs_B | planned | p50 | 423–459 | 427–455 |
| claim_api_S_vs_B | planned | p99 | 2495–9599 | 3391–9471 |
| claim_api_S_vs_B | planned | p999 | 9599–57343 | 12671–26879 |
| fence_A_vs_S | attempt | p50 | 367–407 | 375–427 |
| fence_A_vs_S | attempt | p99 | 447–519 | 447–3423 |
| fence_A_vs_S | attempt | p999 | 3743–5247 | 3743–13567 |
| fence_A_vs_S | planned | p50 | 419–455 | 423–459 |
| fence_A_vs_S | planned | p99 | 1519–6207 | 2495–9599 |
| fence_A_vs_S | planned | p999 | 9727–48639 | 9599–57343 |
| combined_A_vs_B | attempt | p50 | 367–407 | 359–399 |
| combined_A_vs_B | attempt | p99 | 447–519 | 467–3519 |
| combined_A_vs_B | attempt | p999 | 3743–5247 | 3871–14463 |
| combined_A_vs_B | planned | p50 | 419–455 | 427–455 |
| combined_A_vs_B | planned | p99 | 1519–6207 | 3391–9471 |
| combined_A_vs_B | planned | p999 | 9727–48639 | 12671–26879 |

## 判定边界

本报告不自动宣告发布验收通过。64 B 的 GO 只适用于 claim API 吞吐门槛，不包含延迟尾部或发布验收。claim API 结论以 S/B 为准；A/S 单独归因屏障改动；A/B 是组合效果。需要结合预注册阈值、方向一致性、资源事件与完整正式矩阵评审后才能写入 CHANGELOG 或性能报告。
