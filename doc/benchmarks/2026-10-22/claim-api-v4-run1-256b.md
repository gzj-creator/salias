# Claim API v4 三臂 A/B 分析

负载：256 B，FIFO 1P2S，1 MiB，batch=1，poll-limit=64。
B=v3，S=v4+顺序屏障，A=v4+默认非对称屏障。
延迟为各臂 10 轮独立分位数的中位数，不做直方图合并。

## 吞吐

| 对比 | 候选 Mmsg/s | 基线 Mmsg/s | 相对变化 | 组方向 | 精确置换 p |
|---|---:|---:|---:|---:|---:|
| claim_api_S_vs_B | 6.312 | 4.925 | +28.17% | 6/6 | 0.002165 |
| fence_A_vs_S | 7.015 | 6.312 | +11.13% | 6/6 | 0.002165 |
| combined_A_vs_B | 7.015 | 4.925 | +42.44% | 6/6 | 0.002165 |

256 B 结果仅作描述性报告，不套用 64 B GO 门槛。

## 延迟

| 对比 | 口径 | p50 候选/基线 (ns) | p99 候选/基线 (ns) | p99.9 候选/基线 (ns) |
|---|---|---:|---:|---:|
| claim_api_S_vs_B | attempt | 383.0/375.0 (1.021x) | 547.0/563.0 (0.972x) | 7775.0/5183.0 (1.500x) |
| claim_api_S_vs_B | planned | 443.0/415.0 (1.067x) | 3759.0/5023.0 (0.748x) | 11583.0/13695.0 (0.846x) |
| fence_A_vs_S | attempt | 379.0/383.0 (0.990x) | 497.0/547.0 (0.909x) | 4511.0/7775.0 (0.580x) |
| fence_A_vs_S | planned | 439.0/443.0 (0.991x) | 3871.0/3759.0 (1.030x) | 11007.0/11583.0 (0.950x) |
| combined_A_vs_B | attempt | 379.0/375.0 (1.011x) | 497.0/563.0 (0.883x) | 4511.0/5183.0 (0.870x) |
| combined_A_vs_B | planned | 439.0/415.0 (1.058x) | 3871.0/5023.0 (0.771x) | 11007.0/13695.0 (0.804x) |

### 延迟轮间范围

| 对比 | 口径 | 指标 | 候选 min–max (ns) | 基线 min–max (ns) |
|---|---|---|---:|---:|
| claim_api_S_vs_B | attempt | p50 | 205–403 | 367–391 |
| claim_api_S_vs_B | attempt | p99 | 319–1311 | 495–4479 |
| claim_api_S_vs_B | attempt | p999 | 3455–16255 | 3967–14335 |
| claim_api_S_vs_B | planned | p50 | 427–467 | 279–467 |
| claim_api_S_vs_B | planned | p99 | 735–7679 | 2111–9727 |
| claim_api_S_vs_B | planned | p999 | 6591–16383 | 11007–24319 |
| fence_A_vs_S | attempt | p50 | 367–403 | 205–403 |
| fence_A_vs_S | attempt | p99 | 467–2335 | 319–1311 |
| fence_A_vs_S | attempt | p999 | 4063–10751 | 3455–16255 |
| fence_A_vs_S | planned | p50 | 427–467 | 427–467 |
| fence_A_vs_S | planned | p99 | 2655–5887 | 735–7679 |
| fence_A_vs_S | planned | p999 | 8447–14975 | 6591–16383 |
| combined_A_vs_B | attempt | p50 | 367–403 | 367–391 |
| combined_A_vs_B | attempt | p99 | 467–2335 | 495–4479 |
| combined_A_vs_B | attempt | p999 | 4063–10751 | 3967–14335 |
| combined_A_vs_B | planned | p50 | 427–467 | 279–467 |
| combined_A_vs_B | planned | p99 | 2655–5887 | 2111–9727 |
| combined_A_vs_B | planned | p999 | 8447–14975 | 11007–24319 |

## 判定边界

本报告不自动宣告发布验收通过。64 B 的 GO 只适用于 claim API 吞吐门槛，不包含延迟尾部或发布验收。claim API 结论以 S/B 为准；A/S 单独归因屏障改动；A/B 是组合效果。需要结合预注册阈值、方向一致性、资源事件与完整正式矩阵评审后才能写入 CHANGELOG 或性能报告。
