# Reliable FIFO 存活探测优化（2026-10-18）

> 历史归档：仅记录当时版本、配置与结论，不代表当前性能或验收状态。当前汇总及下一轮任务见[最终性能测试与优化交接报告](../../performance-report.md)。

## 状态与范围

状态：子 agent 已交付源码和测试，主 agent 已完成独立构建与验收。
**正确性回归通过，但未达到 50 µs p99.9 验收；性能收益不一致，保留为候选实现而非上线通过版。**
详见 [独立验收报告](tail-latency-acceptance-2026-10-18.md)。本说明的实现阶段未运行负载，
后续编译、回归、A/B 和30分钟长测均由主 agent 执行。按用户后续要求，
实现、测试与精简验收文档作为普通提交保存，不发版，也不表示性能验收通过。

工作目录为 `/data/disk1/projects/salias`，非 GTS 仓库；检查目录祖先及 `src/`、`test/`、
`doc/` 子树未发现适用的 `AGENTS.md`。新增内容沿用中文契约注释。

背景由用户与独立验收方提供：旧 HEAD 为 `5cfcbc9`，冻结 binary 位于
`build/release/tools/hft_acceptance/tail-ab/baseline`；两 publisher、两 subscriber 的
2 秒诊断累计观察到 10,500 次 `/proc/<pid>/stat` 读取，本任务未重新运行该诊断。
既有 30 分钟验收见 `doc/hft-market-data-acceptance-2026-10-18.md`，其中八配置
（2P1S/2P2S × 64/256 B × 稳态/突发）的计划到达时钟、最差 subscriber p99.9
均未达到 50 µs。本补丁不改变这些结论。

主 agent 另负责从 `git archive HEAD` 取得干净基线源码，以同一编译器重建 baseline
与 candidate，避免旧静态库的工具链差异污染 A/B；相关验收工具和
`doc/tail-latency-acceptance-2026-10-18.md` 不在本任务写集内。

## 调用链与修改

生产代码仅修改 `src/core/channel/shared_hybrid_mpsc.hpp`：

- `Tx::claim` 的 Reliable 字节慢路径向 `minimum_consumer_position` 传入本次
  `tail` 与完整帧长 `need`。批量 `Tx::claim_batch` 传入单帧长 `per`：批量允许
  返回安全前缀，不为满足期望批量上限而探测有剩余空间的消费者。
- 字节扫描先 acquire 读取 active 注册 token、核对 `cursor_generation`、读取
  `position`，使用现有 `hybrid_detail::has_capacity` 判定该游标是否阻塞本次申请。
  仅阻塞且具备 PID/start/callback 的消费者调用存活探测。探测后及免探测路径
  都重新 acquire 验证注册 token；已判死也不能越过这一验证。
- `minimum_consumer_message_sequence` 的实际调用点共有三处：非 DropOldest 的
  `Tx` 构造、`claim` 消息窗口慢路径和 `claim_batch` 消息窗口慢路径。均先核对
  代际并读取消息游标，仅当游标超前，或生产者领先消息数已达到启用的窗口上限时
  才探测；构造 Tx 时尚有消息额度也无需探测。
- `src/salias/src/channel.cpp` 无需修改。`SharedControl::is_process_alive` 仍绑定
  `is_registered_process_alive(pid, start_token)`，其 Linux 路径仍读取 `/proc/<pid>/stat`，
  校验启动标识、僵尸状态及既有失败处理；没有换用仅 PID 检查或缓存死亡结论。
- DropOldest/Latest 的 `reliable_consumers_allow_retention`、lease、保留边界及共享
  内存布局不变。消息最小值 helper 只在非 DropOldest 的上述路径被调用；未修改协议。

测试独立放在 `test/channel/shared_reliable_channel_test.cpp`，只向现有
`salias_channel_tests` 的源文件列表增加一行；已有 Latest 测试文件未改。

## 安全论证与边界

1. **不覆盖未读数据。** 对同一注册的字节位置继续 acquire 读取，配对消费者
   release 更新。免探测消费者仍参与最小值，不是假定其死亡或已消费到 producer
   tail；只有该消费者提供的空间足够时才免探测。消息数窗口同理，返回所有参与
   游标的真实最小值；后续申请仍按原容量/窗口逻辑校验。
2. **最慢 Reliable 仍背压。** 阻塞且存活的游标保留在最小值里，缺少探测能力也
   不会被忽略。不同消费者可分别阻塞字节和消息窗口，两个约束必须都满足。
   消费进度在扫描后继续前进只会令当前快照保守，最多延后一次重试。
3. **退出回收不取消。** 阻塞游标每次慢路径仍可探测死亡，并在同一注册快照下
   忽略；没有死亡缓存。尚未阻塞的死亡游标暂时保留在最小值中，额度耗尽后重新
   探测；这不会扩大可写范围。死亡不是通过扫描器从共享注册表注销，原有注册/
   注销流程继续负责回收槽位。
4. **PID 复用不放宽。** 每次必要探测继续传递注册时的 PID 与进程启动标识，
   不能把复用同一 PID 的新进程当成旧消费者；本补丁不调整回调的身份判定规则。
5. **代际与并发注销。** 读取游标前检查 active token 对应的 cursor generation，
   读取后再次核对 token。探测返回死亡时也先检查 token，避免把替换后的新订阅
   误当成死亡旧订阅。生产 API 的 `ClaimReservationGuard`/registration gate 与
   注册、注销端 `wait_for_claims` 完全保留，新增注册从 reservation 快照开始，
   不会把新游标放到已缓存的回收边界之前。
6. **无消费者/全 dead 边界。** 空消费者向量的申请仍返回 `MessageTooLarge`；
   扫描没有参与者（inactive 或全部阻塞者已判 dead）仍回退到 `0`，不改成 tail
   或无限额度。`reliable_only` 的无参与者 sentinel 也保留。已缓存额度及尚不
   阻塞的游标不主动触发死亡判定；优化不是新增即时全进程存活判定保证。
7. **异常共享状态。** 缺失 position、缺失消息窗口游标、缺失/不匹配 generation
   继续保守回退；超前游标不因免探测而获得额外额度。缺少 callback/start 的
   阻塞消费者仍被视作背压来源，不猜测死亡。

## 定向验证与独立结果

新增参数化 fixture `SharedReliableProbeTest`，四个参数组合为字节窗口/消息窗口
与单帧/批量申请。断言 callback 计数而非依赖 wall clock，不访问 `/proc`。
包含单独及同时启用两个窗口、跨物理 ring 容量的连续游标推进、Tx 重建、仅阻塞者
probe、最慢者反复背压、死亡回收及重复探测、PID 启动标识不匹配、all-dead/inactive/
空向量边界、错误或缺失 generation、探测中替换/注销注册、registration gate、
缺少探测能力、借出未 release 的 payload 和批量安全前缀。

注册变化通过 callback 内确定性注入；这些测试不代替真实跨进程注销与退出竞态验收。
交付后主 agent 修正测试空 vector 初始化的编译告警，未修改产品补丁或放宽告警。
定向筛选共111项，109通过、2项单帧参数下不适用的批量测试显式跳过；新fixture
共80实例，78通过、2项上述跳过。全量Release 20/20 CTest、定向ASan/UBSan与TSan
各3/3通过。syscall诊断证实进程探测调用减少，但交错A/B部分配置回退，候选长测
p99.9八阶段全部失败，且一个阶段截止未发送1条。不能据此声称尾延迟优化已整体成功。

本轮定向回归使用的 GTest 过滤器（可复现）：

```text
*SharedReliableProbeTest.*
*SharedReliableProbeTest.*:*ReliableRetentionTest.*:*SharedLatestChannelTest.*
```

主 agent 已完成既有 API 注册、注销、死亡订阅及完整性回归，并按独立 A/B 流程比较八配置。
后续迭代必须继续使用计划到达时钟、逐 subscriber 最差 p99.9、输入完成性/完整性和背压统计，
不能用聚合分位数、减少计划输入、丢旧或更换业务语义替代原验收条件。
