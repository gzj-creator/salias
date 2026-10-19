# Session 4：帧步长与环周转分析

**日期**：2026-10-19（Asia/Singapore）。
**范围**：沿用 Session 1 长测与 Session 3 诊断，不重跑基准、不修改产品 `src/`；本轮完成 Session 4，Session 5/6 尚待接续。

## 源码核验

- `src/core/frame/header.hpp`：`kHeaderSize=8`、`kFrameAlign=8`，不能将 8 字节对齐等同于 64 字节缓存行对齐。
- `src/core/frame/codec.hpp`：`frame_len(payload, has_expiry) = 8 + (has_expiry ? 8 : 0) + align_up(payload, 8)`。
- `tools/aeron_compare/salias_ipc_compare.cpp` 经 `Publisher::try_claim(payload_len)` 进入默认 `has_expiry=false` 的 claim，本次没有 TTL 元数据。
- `src/core/channel/shared_hybrid_mpsc.hpp`：`need = frame::frame_len(payload_len, has_expiry)`，随后 `producer_pos_ = tail + need`；跨物理环尾不额外跳过 padding。
- `src/core/ring/magic_ring.cpp` 与 `src/core/platform/mapping.cpp`：同一 fd 的 offset 0 映射到相邻的两个等长地址区间，slice 使用 `base_ + (pos & mask())`，允许跨尾连续访问。双映射不将逻辑容量翻倍。
- 上述帧、channel、ring、mapping 共六个布局源文件与 Session 1 的基线提交 `c5e6aa7` 内容一致，哈希存入分析 JSON。

无 TTL 时实际步进公式为 **`8 + align_up(payload, 8)`**，不是按缓存行补齐。

## 实测输入与计算结果

输入：`build/benchmarks/baseline-extended-20261019-183822/summary.json` 及同目录 `results.jsonl`。
每个 payload 只使用 Salias、1P2S、吞吐、五轮正式数据（round 1–5）；每轮 100M 消息、1 MiB ring。
工具逐轮校验 stdout SHA-256、RESULT 与 JSONL 一致性、拓扑、交付数及无延迟采样条件。
预热、Aeron、延迟样本不参与本次分析。

| 指标 | 64 B | 256 B |
|---|---:|---:|
| 实际帧步长 | **72 B** | **264 B** |
| 步长 mod 64 | 8 B | 8 B |
| 步长是否为 64 B 倍数 | 否 | 否 |
| 最大在途完整帧数（容量几何上限） | 14,563 | 3,971 |
| 每轮帧总字节数 | 7,200,000,000 | 26,400,000,000 |
| 环周转量（总字节 / 1 MiB） | **6,866.455078** | **25,177.001953** |
| 完成的容量边界跨越数 | 6,866 | 25,177 |
| 环尾 padding | 0 B | 0 B |
| 64 / stride（字节密度） | 0.888889 | 0.242424 |
| 一个 64 B 缓存行可完整容纳的帧数 | 0 | 0 |
| 五轮实测时长范围 | 12.3184–16.3020 s | 6.17634–7.17049 s |
| 全轮平均帧间隔范围（时长 / 消息数） | 123.184–163.020 ns | 61.7634–71.7049 ns |

周转量来自已发布消息数与实际步长的确定性计算，不是额外插桩统计；平均帧间隔也不是 claim 或 Guard 的耗时。
两种步长下，帧头起点均遍历 `0/8/16/24/32/40/48/56 mod 64`，所以 **8 B 帧头自身不跨缓存行**。
完整帧仍跨缓存行；“步长非 64 B 倍数”不等于“帧头发生拆分”。

## 假设与下一步

- 同样 100M 消息时，64 B 周转量仅为 256 B 的 **27.2727%（3/11）**，不是大于两倍；因此不支持“64 B 因更多环周转而变慢”。
- 确认两种帧步长都未按 64 B 对齐，但 **原计划 H2（非对齐导致 64 B 特有瓶颈）未确认**。缺乏相邻 payload、对齐对照与硬件计数器证据，不能宣称缓存未命中或伪共享是根因。
- 不凭此结果批准 Phase 2C padding/对齐优化；是否有收益必须另做语义一致、未插桩的隔离实验。
- 此处 H2 沿用原计划“帧步长非对齐”，不同于 Session 3 提示词中的 H2“消费跟不上”。Session 3 的三个阈值仍均未命中。
- 下一步 **Session 5**：综合保留的基线、本轮 Session 3 诊断和本次布局结果，更新 Phase 1 报告；Phase 2B 仅保留为有条件的发布侧隔离候选，不把它写成已确认根因。

## 产物与复现

- 工具：`tools/aeron_compare/analyze_frame_layout.py`，保持可执行权限。
- JSON：`build/benchmarks/diagnostic-20261019-195710/frame-layout.json`。
- 控制台原始输出：同目录 `frame-layout.stdout`；执行脚本快照：`executed-frame-layout-analyzer.py`。
- JSON SHA-256：`63a8504a07803dfbbf32df14bd4dbe1b6901e1c94b6852afd110582996930f1c`。
- 验证：52 项 Python 回归/IPC 冒烟通过（新增布局测试 12 项），frame/ring/platform 三个 CTest 测试目标全部通过。

```bash
python3 tools/aeron_compare/analyze_frame_layout.py \
  --baseline build/benchmarks/baseline-extended-20261019-183822/summary.json \
  --output build/benchmarks/diagnostic-20261019-195710/frame-layout.json
```

未覆盖 Session 1 的历史 `frame-layout.json`，未改 Session 3 的诊断 JSON；本轮未执行 Git commit、tag 或 push。
