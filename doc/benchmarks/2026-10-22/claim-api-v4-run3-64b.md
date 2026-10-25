# Claim API v4 三臂 A/B 分析

负载：64 B，FIFO 1P2S，1 MiB，batch=1，poll-limit=64。
B=v3，S=v4+顺序屏障，A=v4+默认非对称屏障。
延迟为各臂 10 轮独立分位数的中位数，不做直方图合并。

## 吞吐

| 对比 | 候选 Mmsg/s | 基线 Mmsg/s | 相对变化 | 组方向 | 精确置换 p |
|---|---:|---:|---:|---:|---:|
| claim_api_S_vs_B | 6.206 | 4.981 | +24.59% | 6/6 | 0.002165 |
| fence_A_vs_S | 7.092 | 6.206 | +14.28% | 6/6 | 0.002165 |
| combined_A_vs_B | 7.092 | 4.981 | +42.37% | 6/6 | 0.002165 |

claim API 64 B 预注册吞吐判定：**INCONCLUSIVE**

## 延迟

| 对比 | 口径 | p50 候选/基线 (ns) | p99 候选/基线 (ns) | p99.9 候选/基线 (ns) |
|---|---|---:|---:|---:|
| claim_api_S_vs_B | attempt | 381.0/383.0 (0.995x) | 475.0/475.0 (1.000x) | 4543.0/4319.0 (1.052x) |
| claim_api_S_vs_B | planned | 437.0/433.0 (1.009x) | 3695.0/4175.0 (0.885x) | 11455.0/11519.0 (0.994x) |
| fence_A_vs_S | attempt | 385.0/381.0 (1.010x) | 475.0/475.0 (1.000x) | 4159.0/4543.0 (0.915x) |
| fence_A_vs_S | planned | 429.0/437.0 (0.982x) | 3599.0/3695.0 (0.974x) | 10687.0/11455.0 (0.933x) |
| combined_A_vs_B | attempt | 385.0/383.0 (1.005x) | 475.0/475.0 (1.000x) | 4159.0/4319.0 (0.963x) |
| combined_A_vs_B | planned | 429.0/433.0 (0.991x) | 3599.0/4175.0 (0.862x) | 10687.0/11519.0 (0.928x) |

### 延迟轮间范围

| 对比 | 口径 | 指标 | 候选 min–max (ns) | 基线 min–max (ns) |
|---|---|---|---:|---:|
| claim_api_S_vs_B | attempt | p50 | 203–419 | 371–407 |
| claim_api_S_vs_B | attempt | p99 | 271–9343 | 447–2527 |
| claim_api_S_vs_B | attempt | p999 | 3871–20735 | 4063–12031 |
| claim_api_S_vs_B | planned | p50 | 423–451 | 275–451 |
| claim_api_S_vs_B | planned | p99 | 807–8447 | 1423–6271 |
| claim_api_S_vs_B | planned | p999 | 8703–16895 | 7167–14719 |
| fence_A_vs_S | attempt | p50 | 363–411 | 203–419 |
| fence_A_vs_S | attempt | p99 | 451–1439 | 271–9343 |
| fence_A_vs_S | attempt | p999 | 3551–10495 | 3871–20735 |
| fence_A_vs_S | planned | p50 | 419–455 | 423–451 |
| fence_A_vs_S | planned | p99 | 879–4415 | 807–8447 |
| fence_A_vs_S | planned | p999 | 8191–38911 | 8703–16895 |
| combined_A_vs_B | attempt | p50 | 363–411 | 371–407 |
| combined_A_vs_B | attempt | p99 | 451–1439 | 447–2527 |
| combined_A_vs_B | attempt | p999 | 3551–10495 | 4063–12031 |
| combined_A_vs_B | planned | p50 | 419–455 | 275–451 |
| combined_A_vs_B | planned | p99 | 879–4415 | 1423–6271 |
| combined_A_vs_B | planned | p999 | 8191–38911 | 7167–14719 |

## 判定边界

本报告不自动宣告发布验收通过。64 B 的 GO 只适用于 claim API 吞吐门槛，不包含延迟尾部或发布验收。claim API 结论以 S/B 为准；A/S 单独归因屏障改动；A/B 是组合效果。需要结合预注册阈值、方向一致性、资源事件与完整正式矩阵评审后才能写入 CHANGELOG 或性能报告。
