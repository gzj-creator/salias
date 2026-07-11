# 13 - salias 性能优化复盘

本文总结 salias 从初始共享环实现到最终 hybrid 双模式引擎的主要性能优化，重点说明每项修改要解决的
问题、实现原理和阶段性效果。当前唯一有效的最终性能结论见
`performance-report.md`；本文中的历史数字只用于解释优化过程，不作为当前验收结果。

## 一、最终结论

整个优化过程可以分为四层：

1. 架构重构：从单共享环竞争改成 per-producer 私有环。
2. 共享状态降频：把跨核原子读写从每消息降到每批次或容量不足时。
3. 消费热路径优化：同环连续排空、内联 handler、批量发布进度。
4. 帧头微优化：一次读取、一次提交发布、下一帧预取。

最终推荐配置：

| 模式 | 推荐 batch | salias 中位吞吐 | Aeron 中位吞吐 | salias / Aeron |
|------|-----------:|----------------:|---------------:|---------------:|
| FIFO | 1 | 42.889 Mmsg/s | 37.286 Mmsg/s | 115.0% |
| Ordered | 8 | 35.489 Mmsg/s | 37.993 Mmsg/s | 93.4% |

低延迟场景应另外配置较小的 `publication_window`，避免消息在完整 ring 中长时间排队。

## 二、架构优化

### 1. 单共享环改为 per-producer 私有环

#### 原始问题

单共享环 MPSC/MPMC 要求多个生产者竞争同一个 tail。CAS 失败者反复重试，tail cache line 在多个 CPU
之间迁移，生产者越多，失败重试和 cache coherence 成本越高。实测曾出现增加生产者后聚合吞吐下降的
负扩展。

#### 修改

统一为 hybrid per-producer-ring 引擎：

```text
producer 0 -> ring 0
producer 1 -> ring 1
producer 2 -> ring 2
consumer   <- 合并读取所有 ring
```

每个 producer 独占一个 magic ring，只写自己的生产位置、帧头和 payload。consumer 保存每个 ring 的
独立读位置。

#### 原理

多生产者写入被拆成多个近似 SPSC 数据通路：

- 消除共享 tail CAS 和失败重试。
- 避免多个生产者争夺同一 cache line。
- producer 连续写自己的 ring，具有更好的写缓存局部性。
- 双映射 magic ring 保证跨尾帧仍是连续地址，不需要 term padding、轮转或清零。

#### 效果

它消除了生产者端最大的结构性竞争，并成为后续 FIFO 和 Ordered 分别优化的基础。单共享环 CAS 方案在
当前工作负载下被 per-producer 引擎严格替代。

### 2. FIFO 与 Ordered 拆分语义

#### 原始问题

如果 FIFO 也使用全局序号，生产者每条消息仍要争抢 `global_seq`，为业务不需要的跨生产者全序付费。

#### 修改

引擎提供四种模式：

- `FifoMpsc`
- `FifoFanout`
- `OrderedMpsc`
- `OrderedFanout`

FIFO 使用 producer-local sequence，只保证单 producer 内 FIFO。Ordered 使用：

```cpp
global_seq.fetch_add(1, std::memory_order_relaxed);
```

建立跨 producer 全序。

#### 原理

Aeron 多 publication 的高性能同样来自独立日志，并不提供 publication 之间的全局总序。FIFO 才是与
Aeron MPSC 最接近的语义对比；Ordered 提供 Aeron 没有的额外语义，因此必然承担共享序列化和消费归并
成本。

#### 效果

FIFO producer 热路径实现零共享 RMW。Ordered 保留全序能力，但其剩余瓶颈被明确定位为
`global_seq.fetch_add` 和 consumer 跨环寻找下一全序消息。

### 3. 忙轮询模式移除无效 wake

#### 原始问题

producer commit 后更新共享 `wait_word`，但 `SpinPause` consumer 一直忙轮询，并不需要唤醒。每消息一次
无效 `fetch_add` 会重新制造所有 producer 共享的 cache-line 热点。

#### 修改

```cpp
if (wait_.needs_wake()) {
  wait_word.fetch_add(1, std::memory_order_release);
  wait_.wake(&wait_word);
}
```

`SpinPause::needs_wake()` 为 false，编译器可以消除整个通知分支。

#### 原理与效果

忙轮询 benchmark 的 commit 不再访问共享 `wait_word`，避免通知 cache line 在 producer 之间迁移。只有
真正阻塞等待的策略才支付唤醒成本。

## 三、共享状态降频

### 4. consumer 缓存 `visible_producer_pos`

#### 原始问题

consumer 每读取一条消息都 acquire-load producer 的 `visible_producer_pos`。即使连续读取同一 ring，
仍会重复跨核读取同一个共享位置。

#### 修改

consumer 为每个 ring 保存 `cached_visible_pos`，只有本地读位置追上缓存边界时才重新读取 producer 的
共享发布位置。

#### 原理

一个已经 acquire 到的 visible bound 可以证明该边界之前的 header 都允许检查。在读到边界之前，不需要
重复加载共享位置。共享 acquire 从每消息一次降为每次追上发布边界一次。

### 5. consumer 进度改为批量 flush

#### 原始问题

consumer 每处理一条消息都 release-store 共享消费位置，使 producer 和 consumer 围绕该 cache line
持续通信。

#### 修改

```cpp
consume(message);  // 只更新本地位置
flush_progress();  // 批末统一发布共享位置
```

#### 原理

producer 只在空间可能不足时需要一个足够新的消费位置，不要求每条消息都立即可见。允许共享消费位置短暂
滞后，可以把 N 条消息的 N 次 release store 合并为每个活跃 ring 每批一次。

#### 阶段效果

`cached_visible_pos`、批量进度发布以及相关消费路径整理后：

| 模式 | 优化前 | 优化后 | 提升 |
|------|-------:|-------:|-----:|
| FIFO batch=1 | 13.980 M/s | 19.776 M/s | +41.5% |
| Ordered batch=1 | 10.700 M/s | 16.201 M/s | +51.4% |

## 四、consumer 热路径优化

### 6. `try_recv_run()` 同环连续排空

#### 原始问题

旧流程每读取一条消息就返回上层，再重新扫描所有 producer ring。即使当前 ring 中已有大量连续消息，也会
重复执行 ring 选择、visible 检查、函数进出和共享进度更新。

#### 修改

`try_recv_run(out, cap)` 找到一个可读 ring 后在该 ring 内紧循环读取，直到：

- 达到 `cap`。
- 遇到未提交帧。
- 到达当前 visible bound。
- Ordered 下一全局序号不再属于当前 ring。

#### 原理

FIFO ring 天然形成长连续 run。找到可读 ring 后继续顺序读取，可以保持帧头和 payload 的缓存局部性，
摊薄 ring 扫描与边界判断，并让硬件预取器看到稳定的顺序访问。

### 7. L7 `Subscriber::poll()` handler 内联

#### 原始问题

核心层批量取消息后，上层仍可能逐消息跨 `.so` 或间接调用 handler，产生函数调用、间接跳转和无法内联的
成本。

#### 修改

模板 `Subscriber::poll()` 在调用方编译单元内使用固定消息数组批量接收并直接运行 handler，批末统一
flush 消费进度。

#### 原理

handler 类型在编译期可见后，编译器可以内联 lambda、合并循环控制与 payload 解码，并消除不必要的抽象
边界。

#### Phase A 效果

| 模式 | Phase A 前 | Phase A 后 | 提升 | Phase A 后 / Aeron |
|------|-------------:|-------------:|-----:|--------------------:|
| FIFO | 19.776 M/s | 29.068 M/s | +47.0% | 85.0% |
| Ordered | 16.201 M/s | 17.141 M/s | +5.8% | 51.6% |

FIFO 提升显著，因为同一个 ring 中通常有长 run。Ordered 全序经常在不同 ring 之间交错，同环 run 很短，
因此收益有限。这证明 Ordered 的主要问题不是普通函数调用，而是全序架构。

## 五、帧头与内存访问优化

### 8. 合并 `len + meta` 为单次 64 位 acquire load

#### 原始问题

帧头固定为 8 字节：

```cpp
struct FrameHeader {
  std::uint32_t len;
  std::uint32_t meta;
};
```

旧 consumer 先 acquire-load `meta`，检查 `COMMITTED` 和 sequence，再通过第二次 `slice()` 与
`memcpy()` 读取 `len`，同一个 header 被访问两次。

#### 修改

```cpp
const std::uint64_t header =
    std::atomic_ref<std::uint64_t>(*header_ptr).load(std::memory_order_acquire);
const std::uint32_t len = static_cast<std::uint32_t>(header);
const std::uint32_t meta = static_cast<std::uint32_t>(header >> 32u);
```

#### 原理

release/acquire 同步对象扩展为完整 64 位 header。consumer 一次 acquire 同时得到最终 payload length、
commit flag 和 sequence low bits，省掉第二次地址计算、`slice()` 和 `memcpy()`。

### 9. commit 使用单次 64 位 release store

#### 原始问题

旧流程分别写 `len` 和 `meta`，并在 claim、visible 发布和 commit 阶段执行多次 release store。

#### 修改

claim 阶段：

```cpp
store_header(len, uncommitted_meta, std::memory_order_relaxed);
```

commit 阶段：

```cpp
store_header(len, committed_meta, std::memory_order_release);
```

`visible_producer_pos` 仍在 claim 后 release 发布。

#### 为什么 claim 时仍要写未提交 header

ring 回绕后，当前位置可能残留上一轮带 `COMMITTED` 的旧 header。如果 claim 后推进 visible position，却
不先清除旧状态，consumer 可能把旧帧误认为新帧。因此 claim 必须先写未提交 header，但这一步只需要
relaxed；真正发布 payload 的同步点是 commit 的 release store。

consumer 对完整 header 的 acquire load 与 commit release store 建立 happens-before。看到
`COMMITTED` 时，payload 和最终 `len` 均已可见。

### 10. 下一帧 header 预取

当前帧解析后已经知道：

```cpp
next_position = position + frame_len(payload_len);
```

`try_recv_run()` 使用 `__builtin_prefetch` 提前加载下一帧 header，再执行当前 `Message` 写入、游标更新和
循环控制，以隐藏下一次 acquire-load 的部分缓存缺失延迟。

它最适合 FIFO 的长连续 run；Ordered 只有在批量发布形成同环连续全序段后才能充分利用。

### 最终帧头优化效果

| 模式 | Phase A 后 | 帧头优化后 | 提升 |
|------|-----------:|-----------:|-----:|
| FIFO batch=1 | 29.068 M/s | 42.889 M/s | +47.5% |
| Ordered batch=1 | 17.141 M/s | 22.124 M/s | +29.1% |

FIFO batch=1 本轮达到 Aeron 中位吞吐的 115.0%。新增整环回绕回归测试，确保未提交 claim 不会暴露旧
`COMMITTED` header。

## 六、Ordered 批量发布

### 11. 批量分配全局序号

#### 原始问题

Ordered 每条消息执行一次 `global_seq.fetch_add(1)`，多个 producer 争抢同一 cache line。consumer 还要
在多个 ring 中寻找当前 `expected_sequence`。

#### 修改

`claim_batch()` 一次获取连续全序区间：

```cpp
base_sequence = global_seq.fetch_add(batch_size, std::memory_order_relaxed);
```

同一批消息全部落入当前 producer 的私有 ring。

#### 原理

批量同时减少两笔成本：

1. batch=8 时，8 条消息只执行一次共享 `fetch_add`，平均每消息 RMW 降到八分之一。
2. 一个 producer 获得连续序号后，consumer 可以在同一 ring 中连续读取，减少跨环寻找下一序号。

#### 效果

| Ordered batch | salias | Aeron | salias / Aeron |
|--------------:|-------:|------:|---------------:|
| 1 | 22.124 M/s | 37.026 M/s | 59.8% |
| 8 | 35.489 M/s | 37.993 M/s | 93.4% |
| 16 | 33.182 M/s | 35.179 M/s | 94.3% |

batch=8 比 batch=1 提升约 60.4%。batch=16 的相对比例略高是因为配对 Aeron 中位数不同，但 salias
绝对吞吐低于 batch=8，因此 Ordered 当前推荐 batch=8。

### 12. FIFO 不使用 batch

FIFO producer 已独占 ring，没有 `global_seq` 和共享 tail，不存在需要通过 batch 摊薄的共享 RMW。批量
只会增加生产突发和调度敏感性。

| FIFO batch | salias | 相对 batch=1 |
|-----------:|-------:|-------------:|
| 1 | 42.889 M/s | 基准 |
| 8 | 37.875 M/s | -11.7% |
| 16 | 32.452 M/s | -24.3% |

因此 FIFO 当前推荐 batch=1。

## 七、流控窗口与延迟

### 13. `publication_window` 限制在途数据

#### 原始问题

4 MiB ring 允许 producer 领先 consumer 接近整个 ring。它有利于吞吐，却可能让消息在 ring 中排队数
毫秒。ring capacity 是存储与回绕参数，不应同时决定排队深度。

#### 修改

```cpp
effective_capacity = min(ring_capacity, publication_window);
```

`publication_window == 0` 保持完整环行为。窗口只改变 producer 可领先的字节数，不改变 ring 映射大小、
双映射回绕和 frame 地址计算。

#### 原理

排队延迟近似为：

```text
queue latency ~= inflight bytes / consumer throughput
```

较小窗口让 producer 更早遇到 backpressure，以少量吞吐换取数量级的排队延迟下降。

#### 效果

| FIFO 配置 | p50 | p99 | 吞吐 |
|-----------|----:|----:|-----:|
| 完整 4 MiB 环 | 5374.0 us | 5701.6 us | 21.783 M/s |
| 128 KiB window | 147.5 us | 352.3 us | 19.706 M/s |

p50 降低约 36 倍，p99 从 5.70 ms 降至 352 us，吞吐下降约 9.5%。`publication_window` 是显式的
延迟/吞吐调节旋钮，而不是单纯吞吐优化。

## 八、完整性能演进

### FIFO batch=1

| 阶段 | 吞吐 | 相对上一阶段 |
|------|-----:|-------------:|
| 初始 hybrid 基准 | 13.980 M/s | - |
| visible cache + 批量 flush | 19.776 M/s | +41.5% |
| 同环 run + L7 内联 | 29.068 M/s | +47.0% |
| 64 位 header + 单次 commit + prefetch | 42.889 M/s | +47.5% |

从 13.980 M/s 到 42.889 M/s，总提升约 206.8%，约为初始性能的 3.07 倍。

### Ordered

| 阶段 | 吞吐 | 相对上一阶段 |
|------|-----:|-------------:|
| 初始 hybrid batch=1 | 10.700 M/s | - |
| visible cache + 批量 flush | 16.201 M/s | +51.4% |
| 同环 run + L7 内联 | 17.141 M/s | +5.8% |
| 64 位 header + 单次 commit + prefetch | 22.124 M/s | +29.1% |
| batch=8 | 35.489 M/s | +60.4% |

从初始 10.700 M/s 到 batch=8 的 35.489 M/s，总提升约 231.7%，约为初始性能的 3.32 倍。

不同阶段的 Aeron 配对轮次和虚拟机调度状态不同，阶段百分比用于观察 salias 演进趋势，不应直接相乘。

## 九、核心优化原则

### 1. 消除不必要的共享所有权

- 单共享环改为 producer 私有环。
- FIFO 去掉 `global_seq`。
- busy polling 去掉共享 wake 更新。

共享写入越少，cache coherence 成本越低。

### 2. 将共享同步从每消息摊薄到每批次

- 缓存 `visible_producer_pos`。
- 批量 flush consumer progress。
- Ordered 批量分配 sequence。

原子指令本身不是全部成本，多个核心持续争抢同一 cache line 才是关键问题。

### 3. 利用顺序访问和缓存局部性

- producer 私有连续写入。
- 同环 `try_recv_run()`。
- 下一帧 header prefetch。
- Ordered batch 制造连续全序段。

CPU 最擅长连续且可预测的内存访问。

### 4. 缩短每消息软件路径

- L7 handler 内联。
- 一次 64 位 acquire 读取完整 header。
- 一次 64 位 release 发布完整 header。
- 避免重复 `slice()`、地址计算和 `memcpy()`。

架构竞争消除后，少量额外指令和一次多余访存都会成为主要成本。

### 5. 分离容量、排队和批量职责

- ring capacity 负责存储与回绕。
- publication window 负责最大在途数据和排队延迟。
- batch size 负责摊薄同步成本和控制发布突发。

三个参数不应承担同一个职责。

最终独立进程矩阵覆盖 1/4/64 MiB。FIFO 的最佳绝对吞吐出现在 1/4 MiB；64 MiB 虽然把
salias 背压中位数降至 0，但更大的工作集没有提高吞吐。Ordered batch=8 在 64 MiB 下受益，
说明容量收益与模式、批量和同步结构相关。完整结果见 `performance-report.md`。

因此扩大物理 ring 可以吸收突发并减少空间不足重试，但不能解决长期生产速率高于消费速率的问题，
也不应替代 `publication_window` 对最大在途数据和排队延迟的约束。容量并非越大越快，仍需同时
考虑 cache/TLB 工作集。

## 十、当前边界与下一步

### FIFO

FIFO 已完成主要架构和热路径优化。最终独立进程测试中，2P1S 在 1/4/64 MiB 分别达到
Aeron 的 109.1%、115.5%、176.3%；2P2S 分别达到 192.0%、120.0%、160.2%。当前默认建议：

```text
FIFO batch=1
```

### Ordered

Ordered 的微观成本已经明显降低，但仍有两个结构性成本：

1. producer 竞争 `global_seq.fetch_add`。
2. consumer 在多个 ring 中寻找 `expected_sequence`。

当前吞吐优先建议：

```text
Ordered batch=8
```

如果继续优化 Ordered，应优先考虑：

- 建立 sequence -> producer ring 路由目录，消除 consumer 跨环打猎。
- 或为 Ordered 使用单一共享 MPSC 日志，使全序隐含在 ring position 中。

### 验证边界

最终结果来自 Tencent 4 vCPU x86 KVM：

- Publisher/Subscriber 均为独立可执行程序，不在 benchmark 进程内 `fork()` worker。
- 1/4/64 MiB、2P1S/2P2S、FIFO/Aeron 和 Ordered batch 1/8 每组预热 3 次、正式 20 次。
- 每轮每 Publisher 发布 2,000,000 条 64B 消息，共 480 个正式样本且消息校验全部通过。
- 2P2S Aeron media driver 与一个 Subscriber 共享 CPU，这是 4 vCPU 机器的明确限制。

因此合理结论是 FIFO 在本轮所有容量和拓扑的中位吞吐均超过 Aeron；Ordered batch 模式在提供
更强全局全序语义的同时保持较高吞吐。最终稳定性验收仍应在非超卖 x86 物理机上复跑。
