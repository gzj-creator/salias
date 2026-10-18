# 高频行情合成 IPC 验收：2026-10-18

## 结论与完成状态

**本机合成 FIFO IPC 完整性与输入完成性通过；暂定 50 µs 低尾延迟验收整体不通过。**
以原计划到达时刻为起点、每阶段最差 subscriber 为准，八阶段仅三阶段 p99 通过，
**八阶段 p99.9 全部不通过**。不能据此承诺严格 50 µs 的高频行情关键路径 SLA。
这不是产品不可用的普遍结论，也不是生产 tick-to-trade 验收。

- 实际时间：2026-10-18 **18:21:28–18:51:29 Asia/Singapore**，矩阵墙钟 **1,801.249 秒**。
- 有效负载计划：八阶段 × **225 秒 = 1,800 秒（30 分钟）**；每个拓扑/payload 的稳态与突发各 225 秒，合计 450 秒，**不是每种配置 30 分钟**。
- 当前版本：`5cfcbc99dc2c713a9e67770603fb65230aa24572`，Release `-O3 -DNDEBUG`，GCC 13.3.0，4-vCPU KVM；测量期间没有改源码或覆盖 binary，没有额外并行压测。
- 计划/实际发布均 **2,070,000,000 条（20.7 亿）**；广播后交付 **3,105,000,000 条（31.05 亿）**；所有阶段最终未发送/未交付均为 0。
- 发布 payload **331.2 GB**，逻辑交付 payload **496.8 GB**（十进制、含 48 B 合成 header、不含 salias frame/对齐开销）。接收时延采样 **48,543,540 次**，对应 **32,362,360 条**不同发布消息的采样；广播会对同一发布消息多次计交付样本。
- 每条消息核验 publisher 内序号、重复、缺口、乱序与 payload。所有阶段、每个 subscriber 的错误计数均为 0；worker 无失败，矩阵 exit=0。exit=0 **不等于时延通过**。

最新结果入口见[性能总结](performance-summary-2026-10-18.md)。本轮最终表保留在
[summary.md](benchmarks/2026-10-18/hft-acceptance/summary.md)，逐 subscriber 标量指标、
配置、运行状态及原始文件哈希保留在[精简归档](benchmarks/2026-10-18/performance-results.json)
的 `baseline_long` 项。原始桶和逐秒日志已按用户要求清理，不能再从归档重算历史百分位。

## 主要验收结果

单位 µs；按**计划到达→subscriber 接收、每阶段最差 subscriber**判定。
p99/p99.9 分别要求 ≤50 µs，且输入完成、worker 正常、完整性通过才可通过。
百分位是整个 225 秒阶段直方图的 nearest-rank 桶上界，不取逐轮百分位中位数。

| 拓扑 | payload | 流量 | 最差 p99 | 最差 p99.9 | p99 / p99.9 结论 |
|---|---:|---|---:|---:|---|
| 2P1S | 64 B | 稳态 | 7.423 | 190.463 | 通过 / 未通过 |
| 2P1S | 64 B | 突发 | 9.343 | 503.807 | 通过 / 未通过 |
| 2P1S | 256 B | 稳态 | 11.391 | 1,392.639 | 通过 / 未通过 |
| 2P1S | 256 B | 突发 | 2,555.903 | 37,748.735 | 未通过 / 未通过 |
| 2P2S | 64 B | 稳态 | 1,097.727 | 3,571.711 | 未通过 / 未通过 |
| 2P2S | 64 B | 突发 | 1,294.335 | 31,719.423 | 未通过 / 未通过 |
| 2P2S | 256 B | 稳态 | 1,196.031 | 4,587.519 | 未通过 / 未通过 |
| 2P2S | 256 B | 突发 | 1,605.631 | 6,422.527 | 未通过 / 未通过 |

首次 claim 前起算仅为辅助：例如 2P1S/64 B 稳态的 p99.9 是 **41.983 µs**，
但计划到达口径为 **190.463 µs**。只看前者会漏掉发布前积压/调度迟到，不能据此称阶段 p99.9 通过。
另如 2P2S/256 B 稳态合并后的 first-attempt p99=49.151 µs，但 S1 为 51.711 µs；
不能用跨 subscriber 合并结果掩盖单 subscriber 未达标。

### 负载、背压与积压

每阶段稳态发布 **225,000,000 条**，突发发布 **292,500,000 条**；实际平均发布速率
分别 **1.0M/s、1.3M/s**，两 publisher 平分计划。突发峰值是合成的 **4M/s、持续 100ms/秒**，
不是整阶段 4M/s，也未验证用户真实峰值。1S 每条交付一次，2S 每条广播交付两次。

累计观察到 **1,117,893 次背压失败重试**。2P1S/256 B 突发的 publisher0/1 待发送积压
观察峰值为 **114,835 / 76,847 条**，其计划到达采样最大值 **95,371.495 µs（约95.37ms）**，
是本轮最高采样值。该阶段两 publisher 的背压 episode 累计 **4,202.412ms**，最大单 episode
约 15.30ms；并行进程耗时相加不是额外墙钟。

截止后 subscriber 首次 poll 完成的观察中，2P1S/256 B 突发尚差 1 条，2P2S/256 B 突发
S0 尚差 287 条，随后排空到 0；这不是严格截止瞬间的原子快照。逐秒进度与有界排空结果
均保留，其他阶段该观察为 0。尾延迟尖峰与背压同时存在，**没有调度/宿主证据，不能断言是 KVM、
算法或某单一优化导致**。四种组合只各测一段稳态和突发，不足以估计运行间方差。

### 功能自检与诊断

直方图边界/合并/50µs 阈值 rank、序号缺口/重复/乱序故障检测、稳态/突发逆计划、
100,000 对时钟读取、10,000 次跨进程时钟握手通过。时钟对差 p50=28ns、p99=29ns；
不从时延中减去测量开销。最终源码的四组合 × 两模式短测共八阶段功能正确性通过。
初期短测另注入 subscriber 暂停 200ms，检出 14,402 次背压重试、约 20 万条/producer
待发送积压及约100ms计划到达高尾延迟，证明计划时钟不会因背压重置；该诊断先于最终
接收计时点/自检调整，不用于正式数据比较。短测、诊断、自检均**不计入正式 1,800 秒**。

长测结束后另用冻结 binary 注入 subscriber 暂停 1,500ms、仅计划发布 1 秒：
计划 1,300,000 条，实际发出 992 条、广播交付 1,984 次，明确保留 **1,299,008 条未发送**；
已发送部分完整性通过，但输入完成性为 false，汇总验收判失败。这验证“少发送但采样快”
不能冒充负载完成。另有六项汇总回归测试覆盖：正常通过、慢 subscriber 被聚合掩盖、
worker 失败、输入未完成、完整性失败、坏直方图拒绝，均通过。

## 未覆盖与使用判断

**当前不建议把“p99.9≤50µs”作为这台机器上该配置已验收能力。** 2P1S/64 B 的 p99
在本次合成负载下通过，但 p99.9 仍未通过；2P2S 和 256 B 突发还存在明显毫秒级尾部。
需要先明确业务接受的分位数、负载、拓扑及失效策略，再在实际部署环境复验，不能通过丢旧
消息或改验收口径掩盖失败。

未覆盖真实录制行情、NIC/网络、解码、订单簿/策略、交易所序号域/跨 publisher 全序、
业务缺口恢复/快照、进程崩溃恢复。FIFO 只保证 publisher 内有序，FifoMpmc 是广播，
最慢 Reliable subscriber 可背压 publisher。逐笔/增量行情不能由总条数对齐证明业务正确，
未经业务许可不得切换 DropOldest/latest。

**max 均为采样最大值；未测 p99.99、全消息最坏时延或硬实时上界。** 50µs 是本轮暂定
单跳预算，不代表最终 SLA。本轮仅是合成 IPC 验收，不能称完整高频行情/生产/tick-to-trade 验收。

## 已确定的方法与边界

- 仅测试本机真实跨进程 FIFO IPC，不包含真实行情录制、生产 NIC、行情解码、订单簿或策略，不能称为生产或 tick-to-trade 验收。
- 对当前源码构建 Release；四个组合为 2P1S/2P2S × 64 B/256 B，4 MiB capacity、128 KiB publication_window、batch=1、poll=64，显式绑核。
- 每个组合分别测试稳态与周期突发，各计划测量 225 秒；八阶段计划总测量 1,800 秒，不代表每种配置测了 30 分钟。
- 合成假设：稳态总 1M msg/s；突发每秒前 100ms 总 4M msg/s，其余总 1M msg/s。速率和阶段时长可配置，不代表用户生产峰值。
- 50 µs 是本轮暂定的单跳上限，分别评估 p99 与 p99.9，不替代最终业务 SLA。
- 主验收按原计划到达时刻起算，要求每阶段每个 subscriber 都通过，即按最差 subscriber 判定；首次 claim 前起算仅作为辅助。worker 失败、输入未完成或完整性失败时，不能因采样分位数合格称阶段通过。
- 同时记录首次 claim 前到接收、按计划到达时刻到接收，以及 claim/commit 完成耗时；背压不重新设定计划时钟，不因背压降低计划输入量。
- 逐 publisher/逐 subscriber 校验递增序列、缺口、重复、乱序及 payload；FIFO 不保证跨 publisher 全序，MPMC 为广播而不是竞争消费。
- 保留背压重试及耗时、计划/实际/未发送/未交付消息、逐秒积压与逐订阅者有界直方图；全阶段分位数来自合并桶，不取逐轮百分位中位数。
- 逐笔/增量行情的业务正确性不能由条数一致推出，不使用 DropOldest/latest 掩盖失败，最慢 Reliable subscriber 可能背压 publisher。
- 当前是 4-vCPU KVM。没有调度证据时，不把尖峰直接归因于 KVM 调度。

## 参考基线

旧报告 [publisher-latency-2026-10-18.md](publisher-latency-2026-10-18.md) 的 20 轮仅合计
3.124311 秒，且成功 claim 后才计时。本轮独立补齐持续时间、背压与计划到达口径，
不更改产品代码或旧 compare harness。

历史原始文件已清理；最终 Markdown 与精简标量归档保留，不影响重新运行验收工具。

## 可复现实现与口径

独立实现见 [tools/hft_acceptance/README.md](../tools/hft_acceptance/README.md)。
不接入产品库的新逻辑，也不修改旧 compare harness。可直接重现：

```bash
bash tools/hft_acceptance/build.sh
build/release/tools/hft_acceptance/acceptance --self-test
bash tools/hft_acceptance/run_matrix.sh /tmp/hft-acceptance 225
python3 tools/hft_acceptance/summarize.py /tmp/hft-acceptance
python3 tools/hft_acceptance/test_summary.py
```

原运行口径的复跑命令如下；新输出放到 Git 忽略的 `build/benchmarks/`，不写入文档目录：

```bash
out=build/benchmarks/hft-acceptance/$(date +%Y%m%d-%H%M%S)
mkdir -p "$out"
bash tools/hft_acceptance/capture_environment.sh > "$out/environment.txt"
build/release/tools/hft_acceptance/acceptance --self-test > "$out/self-test.jsonl"
bash tools/hft_acceptance/run_matrix.sh "$out" 225
python3 tools/hft_acceptance/summarize.py "$out"
```

测量源码 SHA256：`a3e702d190931edd3ac1fd2c631bc7499786e628d617439cec4b1f7dea6f8659`；
binary SHA256：`fa4df68c8af3b3353d2a65260ee8ec11fe921cacaa01db1da7ace9f5acc9da94`。
二者在长测前后相同。只有汇总脚本/文档在运行中完善。

本轮曾记录环境、自检、精确命令、stderr、逐秒进度和稀疏直方图，并在清理前重新校验
原始数据与最终汇总一致。现只保留最终表及精简归档；原始日志、桶及重复的大 JSON 已删除。
保留的哈希仅供来源识别，不是数据备份；历史百分位重新计算需要原始桶，现已不可执行。

### 开环与背压

每个 publisher 的消息序号对应固定计划时刻。背压只会积累未发送任务，不会重置
计划时钟；追赶期间继续用原计划时刻。到 225 秒截止时停止新尝试，未发送数保留；
已成功 claim 可完成 commit，订阅者最多追加 5 秒排空已发布消息。排空不计入
225 秒负载时长，不以尾延迟合格掩盖输入未完成。

订阅回调在复制 48 B header 后、做序号和 payload 检查前读时钟；从首次 claim 前
起算的口径包含重试、写 payload 和 commit；从计划时刻起算的口径还包含 publisher
被延迟调度及待发送积压。两种口径都不包含网络/解码/订单簿/策略。

每条交付校验 publisher、序号、header checksum、原计划时间戳及所有剩余 payload
字。校验工作本身会占用 subscriber CPU 并影响后续消息，不能当作零开销测量。

### 采样、百分位与积压

序号混合哈希约 1/64 采样，避免固定采样周期与突发相位锁定；报告实际样本数。
采用现有 3,776 桶直方图，非精确区每个二次幂区间 64 子桶；分位数为桶上界，
max 是采样 max，不是所有消息的最坏时延。独立精确计数 sampled latency >50,000ns，
按 nearest-rank 判 p99/p99.9 是否达标，避免桶舍入影响边界判断。
本次不测 p99.99，也没有逐条读接收时钟以证明全消息最坏时延；不能把采样 max
当作硬实时上界。矩阵 exit=0 仅表示执行和完整性检查成功，不表示时延 SLA 通过。

逐秒积压来自非原子整体快照；publisher 每 256 条刷新计数，观察值不是瞬时精确
峰值。截止未收数是在 subscriber 截止后首次 poll 完成时观察，可能已含少量排空。
最终未发送、未交付、去重序号数才是精确完成性判断依据。

CPU 绑定：2P1S 的 subscriber=CPU0、publisher=CPU1/2；2P2S 的 subscriber=CPU0/1、
publisher=CPU2/3。绑核不等于 CPU 隔离；父进程采集与系统后台服务仍可能竞争资源。

## 交付与清理

新增文件仅位于 `tools/hft_acceptance/`、本报告和原始证据目录；没有更改产品代码、
旧 compare harness、CMake 或既有未提交修改，没有 Git commit。最终做 `git diff --check`、
原始消息/桶/分位数复算、六项汇总回归、shell 语法检查与 SHA256 前后比较。
八阶段长测会话已退出；所有本任务创建的 publisher/subscriber/timeout 已回收，
`/dev/shm` 无本任务残留。最终复核记录和文件校验清单位于证据目录。
