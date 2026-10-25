# Claim API v4 三臂 A/B 分析

负载：256 B，FIFO 1P2S，1 MiB，batch=1，poll-limit=64。
B=v3，S=v4+顺序屏障，A=v4+默认非对称屏障。
延迟为各臂 10 轮独立分位数的中位数，不做直方图合并。

## 吞吐

| 对比 | 候选 Mmsg/s | 基线 Mmsg/s | 相对变化 | 组方向 | 精确置换 p |
|---|---:|---:|---:|---:|---:|
| claim_api_S_vs_B | 6.348 | 4.806 | +32.09% | 6/6 | 0.002165 |
| fence_A_vs_S | 7.092 | 6.348 | +11.72% | 6/6 | 0.002165 |
| combined_A_vs_B | 7.092 | 4.806 | +47.58% | 6/6 | 0.002165 |

256 B 结果仅作描述性报告，不套用 64 B GO 门槛。

## 延迟

| 对比 | 口径 | p50 候选/基线 (ns) | p99 候选/基线 (ns) | p99.9 候选/基线 (ns) |
|---|---|---:|---:|---:|
| claim_api_S_vs_B | attempt | 381.0/375.0 (1.016x) | 483.0/529.0 (0.913x) | 4383.0/4671.0 (0.938x) |
| claim_api_S_vs_B | planned | 449.0/425.0 (1.056x) | 3327.0/4319.0 (0.770x) | 11199.0/13887.0 (0.806x) |
| fence_A_vs_S | attempt | 375.0/381.0 (0.984x) | 479.0/483.0 (0.992x) | 4159.0/4383.0 (0.949x) |
| fence_A_vs_S | planned | 445.0/449.0 (0.991x) | 3439.0/3327.0 (1.034x) | 10879.0/11199.0 (0.971x) |
| combined_A_vs_B | attempt | 375.0/375.0 (1.000x) | 479.0/529.0 (0.905x) | 4159.0/4671.0 (0.890x) |
| combined_A_vs_B | planned | 445.0/425.0 (1.047x) | 3439.0/4319.0 (0.796x) | 10879.0/13887.0 (0.783x) |

### 延迟轮间范围

| 对比 | 口径 | 指标 | 候选 min–max (ns) | 基线 min–max (ns) |
|---|---|---|---:|---:|
| claim_api_S_vs_B | attempt | p50 | 371–399 | 363–423 |
| claim_api_S_vs_B | attempt | p99 | 471–2335 | 491–4287 |
| claim_api_S_vs_B | attempt | p999 | 3871–13311 | 3903–15103 |
| claim_api_S_vs_B | planned | p50 | 427–467 | 407–467 |
| claim_api_S_vs_B | planned | p99 | 1199–4607 | 831–8703 |
| claim_api_S_vs_B | planned | p999 | 7615–17151 | 7807–704511 |
| fence_A_vs_S | attempt | p50 | 363–387 | 371–399 |
| fence_A_vs_S | attempt | p99 | 463–503 | 471–2335 |
| fence_A_vs_S | attempt | p999 | 2175–4415 | 3871–13311 |
| fence_A_vs_S | planned | p50 | 263–463 | 427–467 |
| fence_A_vs_S | planned | p99 | 783–4735 | 1199–4607 |
| fence_A_vs_S | planned | p999 | 6527–13951 | 7615–17151 |
| combined_A_vs_B | attempt | p50 | 363–387 | 363–423 |
| combined_A_vs_B | attempt | p99 | 463–503 | 491–4287 |
| combined_A_vs_B | attempt | p999 | 2175–4415 | 3903–15103 |
| combined_A_vs_B | planned | p50 | 263–463 | 407–467 |
| combined_A_vs_B | planned | p99 | 783–4735 | 831–8703 |
| combined_A_vs_B | planned | p999 | 6527–13951 | 7807–704511 |

## 判定边界

本报告不自动宣告发布验收通过。64 B 的 GO 只适用于 claim API 吞吐门槛，不包含延迟尾部或发布验收。claim API 结论以 S/B 为准；A/S 单独归因屏障改动；A/B 是组合效果。需要结合预注册阈值、方向一致性、资源事件与完整正式矩阵评审后才能写入 CHANGELOG 或性能报告。
