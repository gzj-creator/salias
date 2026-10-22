# Claim API v4 三臂 A/B 分析

负载：256 B，FIFO 1P2S，1 MiB，batch=1，poll-limit=64。
B=v3，S=v4+顺序屏障，A=v4+默认非对称屏障。
延迟为各臂 10 轮独立分位数的中位数，不做直方图合并。

## 吞吐

| 对比 | 候选 Mmsg/s | 基线 Mmsg/s | 相对变化 | 组方向 | 精确置换 p |
|---|---:|---:|---:|---:|---:|
| claim_api_S_vs_B | 6.294 | 4.877 | +29.07% | 6/6 | 0.002165 |
| fence_A_vs_S | 7.038 | 6.294 | +11.82% | 6/6 | 0.002165 |
| combined_A_vs_B | 7.038 | 4.877 | +44.32% | 6/6 | 0.002165 |

256 B 结果仅作描述性报告，不套用 64 B GO 门槛。

## 延迟

| 对比 | 口径 | p50 候选/基线 (ns) | p99 候选/基线 (ns) | p99.9 候选/基线 (ns) |
|---|---|---:|---:|---:|
| claim_api_S_vs_B | attempt | 389.0/377.0 (1.032x) | 501.0/513.0 (0.977x) | 4607.0/6047.0 (0.762x) |
| claim_api_S_vs_B | planned | 443.0/431.0 (1.028x) | 4639.0/4543.0 (1.021x) | 14079.0/14143.0 (0.995x) |
| fence_A_vs_S | attempt | 381.0/389.0 (0.979x) | 1247.0/501.0 (2.489x) | 9471.0/4607.0 (2.056x) |
| fence_A_vs_S | planned | 433.0/443.0 (0.977x) | 4479.0/4639.0 (0.966x) | 14079.0/14079.0 (1.000x) |
| combined_A_vs_B | attempt | 381.0/377.0 (1.011x) | 1247.0/513.0 (2.431x) | 9471.0/6047.0 (1.566x) |
| combined_A_vs_B | planned | 433.0/431.0 (1.005x) | 4479.0/4543.0 (0.986x) | 14079.0/14143.0 (0.995x) |

### 延迟轮间范围

| 对比 | 口径 | 指标 | 候选 min–max (ns) | 基线 min–max (ns) |
|---|---|---|---:|---:|
| claim_api_S_vs_B | attempt | p50 | 375–403 | 203–387 |
| claim_api_S_vs_B | attempt | p99 | 471–551 | 495–2271 |
| claim_api_S_vs_B | attempt | p999 | 4287–9727 | 4063–1048575 |
| claim_api_S_vs_B | planned | p50 | 431–483 | 275–443 |
| claim_api_S_vs_B | planned | p99 | 1263–9855 | 2655–9215 |
| claim_api_S_vs_B | planned | p999 | 8959–868351 | 9727–190463 |
| fence_A_vs_S | attempt | p50 | 191–415 | 375–403 |
| fence_A_vs_S | attempt | p99 | 303–4351 | 471–551 |
| fence_A_vs_S | attempt | p999 | 751–14207 | 4287–9727 |
| fence_A_vs_S | planned | p50 | 415–447 | 431–483 |
| fence_A_vs_S | planned | p99 | 2847–7359 | 1263–9855 |
| fence_A_vs_S | planned | p999 | 10751–245759 | 8959–868351 |
| combined_A_vs_B | attempt | p50 | 191–415 | 203–387 |
| combined_A_vs_B | attempt | p99 | 303–4351 | 495–2271 |
| combined_A_vs_B | attempt | p999 | 751–14207 | 4063–1048575 |
| combined_A_vs_B | planned | p50 | 415–447 | 275–443 |
| combined_A_vs_B | planned | p99 | 2847–7359 | 2655–9215 |
| combined_A_vs_B | planned | p999 | 10751–245759 | 9727–190463 |

## 判定边界

本报告不自动宣告发布验收通过。64 B 的 GO 只适用于 claim API 吞吐门槛，不包含延迟尾部或发布验收。claim API 结论以 S/B 为准；A/S 单独归因屏障改动；A/B 是组合效果。需要结合预注册阈值、方向一致性、资源事件与完整正式矩阵评审后才能写入 CHANGELOG 或性能报告。
