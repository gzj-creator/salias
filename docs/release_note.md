# Release Notes

本文件按时间顺序记录每次发版，每个版本节与同名的 git tag 对齐。tag 注解内容以此处对应版本节为唯一真源。

## v1.2.0 - 2026-07-11

- **版本级别**：次版本（minor）
- **Git 提交消息**：chore: 发版 v1.2.0
- **git tag**：v1.2.0
- **自上次 tag（v1.1.0）以来的变更摘要**：
  - **新增**：独立进程的 salias / Aeron 基准 worker（Publisher / Subscriber），由外部协调而非 fork；最终矩阵覆盖 2P1S/2P2S、1/4/64 MiB 容量、FIFO 公平、Ordered batch 1/8、消息校验与背压统计。
  - **新增**：独立交互式 HTML 动画，直观说明 Channel、Publisher/Subscriber、Producer/Consumer、per-producer MagicRing、顺序、fanout、release 与背压的消息流。
  - **变更**：用一份独立进程最终性能报告替代历史对比文档与日志，包含机器配置、480 份校验过的原始样本、生成的摘要，以及内嵌在 README 的核心 salias/Aeron 对照表。
  - **变更**：更新优化复盘，将独立进程最终报告作为当前唯一的性能真相来源。
  - **杂项**：将 `*.log` 加入 `.gitignore`，基准与运行时日志不再纳入版本库。
  - **移除**：移除被替代的 Tencent/Aeron 对比报告与采用旧测试框架或冲突测量条件的中间基准日志。
