# Claim API v4 三臂 A/B 分析

负载：64 B，FIFO 1P2S，1 MiB，batch=1，poll-limit=64。
B=v3，S=v4+顺序屏障，A=v4+默认非对称屏障。
延迟为各臂 10 轮独立分位数的中位数，不做直方图合并。

## 吞吐

| 对比 | 候选 Mmsg/s | 基线 Mmsg/s | 相对变化 | 组方向 | 精确置换 p |
|---|---:|---:|---:|---:|---:|
| claim_api_S_vs_B | 6.139 | 4.957 | +23.84% | 6/6 | 0.002165 |
| fence_A_vs_S | 7.044 | 6.139 | +14.74% | 6/6 | 0.002165 |
| combined_A_vs_B | 7.044 | 4.957 | +42.10% | 6/6 | 0.002165 |

claim API 64 B 预注册吞吐判定：**INCONCLUSIVE**

## 延迟

| 对比 | 口径 | p50 候选/基线 (ns) | p99 候选/基线 (ns) | p99.9 候选/基线 (ns) |
|---|---|---:|---:|---:|
| claim_api_S_vs_B | attempt | 385.0/387.0 (0.995x) | 471.0/483.0 (0.975x) | 4287.0/4287.0 (1.000x) |
| claim_api_S_vs_B | planned | 447.0/439.0 (1.018x) | 3375.0/4079.0 (0.827x) | 12351.0/12799.0 (0.965x) |
| fence_A_vs_S | attempt | 381.0/385.0 (0.990x) | 487.0/471.0 (1.034x) | 8959.0/4287.0 (2.090x) |
| fence_A_vs_S | planned | 431.0/447.0 (0.964x) | 2407.0/3375.0 (0.713x) | 12735.0/12351.0 (1.031x) |
| combined_A_vs_B | attempt | 381.0/387.0 (0.984x) | 487.0/483.0 (1.008x) | 8959.0/4287.0 (2.090x) |
| combined_A_vs_B | planned | 431.0/439.0 (0.982x) | 2407.0/4079.0 (0.590x) | 12735.0/12799.0 (0.995x) |

### 延迟轮间范围

| 对比 | 口径 | 指标 | 候选 min–max (ns) | 基线 min–max (ns) |
|---|---|---|---:|---:|
| claim_api_S_vs_B | attempt | p50 | 379–407 | 367–399 |
| claim_api_S_vs_B | attempt | p99 | 451–1151 | 459–2975 |
| claim_api_S_vs_B | attempt | p999 | 3839–10623 | 3743–12415 |
| claim_api_S_vs_B | planned | p50 | 435–459 | 275–451 |
| claim_api_S_vs_B | planned | p99 | 1087–5823 | 1247–5503 |
| claim_api_S_vs_B | planned | p999 | 7743–2064383 | 9983–23039 |
| fence_A_vs_S | attempt | p50 | 367–403 | 379–407 |
| fence_A_vs_S | attempt | p99 | 471–2527 | 451–1151 |
| fence_A_vs_S | attempt | p999 | 4031–12671 | 3839–10623 |
| fence_A_vs_S | planned | p50 | 419–447 | 435–459 |
| fence_A_vs_S | planned | p99 | 1183–6207 | 1087–5823 |
| fence_A_vs_S | planned | p999 | 7679–729087 | 7743–2064383 |
| combined_A_vs_B | attempt | p50 | 367–403 | 367–399 |
| combined_A_vs_B | attempt | p99 | 471–2527 | 459–2975 |
| combined_A_vs_B | attempt | p999 | 4031–12671 | 3743–12415 |
| combined_A_vs_B | planned | p50 | 419–447 | 275–451 |
| combined_A_vs_B | planned | p99 | 1183–6207 | 1247–5503 |
| combined_A_vs_B | planned | p999 | 7679–729087 | 9983–23039 |

## 判定边界

本报告不自动宣告发布验收通过。64 B 的 GO 只适用于 claim API 吞吐门槛，不包含延迟尾部或发布验收。claim API 结论以 S/B 为准；A/S 单独归因屏障改动；A/B 是组合效果。需要结合预注册阈值、方向一致性、资源事件与完整正式矩阵评审后才能写入 CHANGELOG 或性能报告。
