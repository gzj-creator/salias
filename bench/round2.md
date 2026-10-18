# Round 2 验收工具

`salias_bench_round2` 使用 Google Benchmark，参数顺序为 Latest 的
`policy/consumers/message_window/payload_bytes`（policy 0=DropOldest、1=Reliable），
以及 Priority 的 `wait_policy/payload_bytes`（0=Yield、1=Spin、2=Adaptive）。

```bash
cmake --preset release
cmake --build --preset release --target salias_bench_round2 salias_round2_probe
./build/release/bench/salias_bench_round2 \
  --benchmark_min_time=0.1s --benchmark_repetitions=5 \
  --benchmark_report_aggregates_only=true \
  --benchmark_out=/tmp/salias-round2.json --benchmark_out_format=json
```

`salias_round2_probe` 不依赖 Google Benchmark，逐条核对消息序号和完整载荷，
每次运行输出一行 JSON，错误返回非零。内置进程 alarm；正式验收仍建议外部 timeout。
主场景至少 30 秒、重复 3 次，性能场景应串行运行，避免并行构建或其他压测干扰。

```bash
# Reliable: seconds writers readers message_window payload_bytes batch
timeout 50s ./build/release/bench/salias_round2_probe reliable 30 4 4 0 64 1
timeout 50s ./build/release/bench/salias_round2_probe reliable 30 4 8 16 64 4
# Priority 有积压连续发布：seconds wait_policy，固定 2 流、64B
timeout 50s ./build/release/bench/salias_round2_probe priority-saturated 30 0
# Priority 单条握手唤醒：seconds wait_policy interval_us bytes streams topology
timeout 50s ./build/release/bench/salias_round2_probe priority 30 2 2000 64 2 cores
# 空闲 CPU：间隔 5 秒，在最后一次收取后停止；需对比 Yield 与 Adaptive
timeout 25s ./build/release/bench/salias_round2_probe priority 5 2 5000000 64 2 cores
# 不使用 benchmark 框架的固定消息数，用于 strace 系统调用次数差分
strace -e trace=openat -o /tmp/salias-trace.log \
  ./build/release/bench/salias_round2_probe trace 10000 0
```

Reliable 支持 1–4 个 writer、1–8 个 reader、8–1024B、1–16 条 batch；
batch 必须能放入 2048B 保留窗口，消息窗口启用时 batch 不大于该窗口。
Priority 的 stream 数支持 2、3、8；topology 为 `thread`（不绑核）、`process`
（跨进程）、`same`（同一可用 CPU）、`cores`（两个不同的可用 CPU，Linux）。
唤醒工具使用逐条 ack，报告 p50/p99/p99.9、接收线程 CPU 和 wall；它不是积压吞吐测试。
连续吞吐工具在结束时发送每流终止帧，排空后核对发布与接收总数。

`priority` 的延迟值是从发布前时间戳到接收完成的跨线程/跨核唤醒时延，负载按
`interval_us` 稀疏发布并等待接收端 ack；Adaptive 会让接收线程进入 futex 睡眠，系统调度与
跨核唤醒通常以微秒计。它不能和连续轮询的 `salias_hotpath_ab` `ns_per_msg` 直接比较。
判断 ring 热路径是否回归时，必须使用同一工具、同一 workload 和同一构建配置做前后对照；
判断 Adaptive 的收益时，比较同一稀疏场景中 Yield/Spin/Adaptive 的 CPU 与分位延迟。

对冻结的旧版源码比较时，必须单独构建其 `salias` 和 `salias_core` 静态库，
再以相同编译参数、最终版工具源码和 `-DSALIAS_ROUND2_BASELINE` 编译工具，
include 路径和静态库均指向冻结副本，不能混用当前工作树的库。
该宏只把 Priority 调用切回旧 `recv()`；旧版所有策略标签实际均为 Yield，
因此只将 policy=0 用作 Yield 基线。其他测量与载荷校验保持一致。

微基准不是生产时延承诺；报告 CPU 拓扑、亲和性、编译参数、benchmark 库构建类型、
中位数和离散度。`strace` 只用于调用次数，不用于测量时延。
