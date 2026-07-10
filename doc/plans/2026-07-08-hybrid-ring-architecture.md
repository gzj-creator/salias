# Hybrid Ring Architecture Design (超越 Aeron)

Date: 2026-07-08  
Status: Design  
Goal: 架构重构，通过 per-producer magic ring + lightweight sequencing 实现对 Aeron 的全面超越

---

## 1. 问题诊断

### 1.1 当前架构的致命缺陷

**单点 CAS 瓶颈**（`shared_mpsc.hpp:89-90`）：
```cpp
std::atomic_ref<std::uint64_t>(*channel_->reserved_tail_)
    .compare_exchange_strong(reserved_tail, next_tail, ...)
```

所有生产者竞争同一个 `reserved_tail` 原子变量导致：
- **Cache line ping-pong**：64B cache line 在 N 个 CPU 核心间来回弹跳
- **CAS 成功率 = 1/N**：4 个生产者时，每个生产者平均失败 3 次才成功 1 次
- **负扩展**：4P 总吞吐量仅为 1P 的 51%（实测数据）

**性能数据（Tencent CVM）**：

| Scenario | salias 当前 | Aeron IPC | salias/Aeron |
|----------|-------------|-----------|--------------|
| 1P/1C batch=1 | 26.6 M/s | 35 M/s | 0.76x |
| 4P/1C batch=1 | 13.5 M/s (aggregate) | 16.5 M/s | 0.82x |
| 4P/1C batch=16 | 35 M/s | ~25 M/s | 1.40x |

**关键发现**：
- 即使修复 CAS backoff bug，单生产者仍落后 24%
- 多生产者 batch=1 时出现负扩展（4P 总吞吐 < 1P 的 2 倍）
- Batch=16 时已经领先，说明**问题在 CAS 频率**，不在 magic ring 本身

### 1.2 Aeron 的架构优势与局限

**Aeron ExclusivePublication 的优势**：
- 每个生产者独占一个 term buffer
- 生产者内部无竞争（单调递增 tail）
- 只在 term rotation 时需要协调

**Aeron 的局限（我们的机会）**：
- **仍需 Media Driver 做流控协调**（控制面依赖）
- **Term rotation 的清零抖动**（64MB memset 导致 p99.9 尖峰）
- **32 字节 header + 32 字节对齐**（小消息开销大）
- **大消息分片重组**（FragmentAssembler 引入拷贝）
- **消费者需要 merge 多个 log buffer**（cursor 合并开销）

**salias 已有的结构性优势**（Aeron 无法模仿）：
1. **Magic ring buffer**：双映射消除 term rotation、零抖动、零分片
2. **8 字节 header**：有效载荷利用率更高
3. **Driverless**：无 Media Driver 依赖
4. **C++23 无 GC**：无 JVM 的 GC/安全点抖动

---

## 2. 架构设计：Hybrid Ring

### 2.1 核心思想

**结合两者优势**：
- **Per-producer magic ring**：每个生产者独占一个双映射 ring，写入零竞争
- **Lightweight global sequencer**：用轻量的 `fetch_add` 分配全局序列号（比 CAS 区间预留快 10 倍）
- **Sequence-ordered consumption**：消费者按序列号有序读取，保证消息顺序

**关键洞察**：
- 预留**区间**需要 `compare_exchange(tail, tail+len)`（重量级，失败需重试）
- 分配**序列号**只需 `fetch_add(1)`（轻量级，永不失败）
- 生产者在私有 ring 上写入时完全无竞争

### 2.2 数据结构

```cpp
namespace salias::channel {

// 每个生产者的私有 ring（独立物理内存，双映射）
struct ProducerRing {
  platform::Mapping mapping;       // RAII：持有 fd 和双映射基址
  ring::MagicRing ring;            // 基于 mapping 的 magic ring
  
  // 生产者私有计数器（仅此生产者写，无竞争）
  alignas(128) std::uint64_t producer_pos = 0;
  std::uint64_t cached_consumer_seq = 0;   // 缓存的消费者进度
  
  // 统计信息（调试/监控用）
  std::uint64_t total_written = 0;
  std::uint64_t backpressure_count = 0;
};

// Hybrid MPSC Channel
template <wait::WaitStrategy Wait = wait::SpinPause>
class HybridMpscChannel {
 public:
  struct Config {
    std::uint32_t num_producers = 1;
    std::size_t ring_capacity_per_producer = 4 * 1024 * 1024;  // 4MB
    platform::HugePage huge = platform::HugePage::None;
    int numa_node = -1;
  };
  
  static std::expected<HybridMpscChannel, ChannelError> create(Config cfg);
  
  // 生产者端点
  class Tx {
   public:
    // 单消息 claim
    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept;
    
    // 批量 claim（一次 fetch_add 分配 N 个序列号）
    BatchClaimResult claim_batch(std::uint32_t payload_len, 
                                  std::uint32_t max_frames) noexcept;
    
    void commit(const flow::Claim& claim) noexcept;
    void commit_batch(const flow::BatchClaim& batch) noexcept;
  };
  
  // 消费者端点
  class Rx {
   public:
    // 按序列号顺序 poll
    std::uint32_t poll(flow::MessageHandler auto handler, 
                       std::uint32_t limit) noexcept;
  };
  
 private:
  // 共享控制块（跨进程共享内存）
  struct alignas(128) SharedControl {
    // 全局序列号生成器（唯一的共享原子变量）
    alignas(128) std::atomic<std::uint64_t> global_seq{0};
    
    // 消费者进度（消费者独占写）
    alignas(128) std::atomic<std::uint64_t> consumer_seq{0};
    
    // 等待字（futex）
    alignas(128) std::atomic<std::uint32_t> wait_word{0};
    
    // 生产者数量（初始化后不变）
    std::uint32_t num_producers = 0;
  };
  
  platform::Mapping control_mapping_;   // 控制块共享内存
  SharedControl* control_ = nullptr;
  
  std::vector<ProducerRing> producer_rings_;  // 每个生产者的私有 ring
  Wait wait_;
};

} // namespace salias::channel
```

**内存布局要点**：
- `global_seq` / `consumer_seq` / `wait_word` 各自独占 128B（防止 false sharing）
- 每个 `ProducerRing` 持有独立的物理内存（通过 `platform::Mapping`）
- `producer_pos` 是生产者线程本地的，不在共享内存里（零 cache line 竞争）

### 2.3 Claim 算法（生产者）

```cpp
flow::Producer::ClaimResult HybridMpscChannel::Tx::claim(std::uint32_t payload_len) noexcept {
  auto& my_ring = channel_->producer_rings_[producer_id_];
  const std::size_t need = frame::frame_len(payload_len);
  
  if (need > my_ring.ring.capacity()) {
    return std::unexpected(flow::FlowError::MessageTooLarge);
  }
  
  // 步骤 1：检查私有 ring 是否有空间（无竞争）
  if (!has_capacity(my_ring, need)) {
    // 刷新缓存的消费者进度
    my_ring.cached_consumer_seq = 
      channel_->control_->consumer_seq.load(std::memory_order_acquire);
    
    if (!has_capacity(my_ring, need)) {
      ++my_ring.backpressure_count;
      return std::unexpected(flow::FlowError::BackPressured);
    }
  }
  
  // 步骤 2：分配全局序列号（轻量 fetch_add，永不失败）
  const std::uint64_t seq = 
    channel_->control_->global_seq.fetch_add(1, std::memory_order_relaxed);
  
  // 步骤 3：在私有 ring 上预留空间（完全无竞争）
  const std::uint64_t ring_pos = my_ring.producer_pos;
  my_ring.producer_pos += need;
  
  // 步骤 4：写入帧头（未提交状态）
  const std::uint32_t meta = 
    frame_meta(ring_pos, my_ring.ring.capacity(), seq, false);
  write_uncommitted_header(my_ring.ring, ring_pos, payload_len, meta);
  
  return flow::Claim{
    .payload = my_ring.ring.slice_mut(ring_pos + frame::kHeaderSize, payload_len),
    .start_pos = ring_pos,
    .payload_len = payload_len,
    .meta = meta,
    .sequence = seq,  // 新增字段
  };
}

// 检查私有 ring 容量（无原子操作）
bool has_capacity(ProducerRing& ring, std::size_t need) noexcept {
  const std::uint64_t used = ring.producer_pos - ring.cached_consumer_seq;
  return used <= ring.ring.capacity() && need <= ring.ring.capacity() - used;
}
```

**关键改进**：
- ✅ `fetch_add(1)` 比 `compare_exchange(tail, tail+len)` 快约 10 倍
- ✅ `fetch_add` 永不失败，无需 backoff 重试
- ✅ 在私有 ring 上写入时完全无竞争
- ✅ 只在 backpressure 时才读取共享的 `consumer_seq`

### 2.4 Batch Claim 优化

```cpp
BatchClaimResult HybridMpscChannel::Tx::claim_batch(
    std::uint32_t payload_len, std::uint32_t max_frames) noexcept {
  
  auto& my_ring = channel_->producer_rings_[producer_id_];
  const std::size_t per = frame::frame_len(payload_len);
  
  // 计算实际能容纳的帧数
  std::uint32_t fit = calc_batch_fit(my_ring, per, max_frames);
  if (fit == 0) {
    my_ring.cached_consumer_seq = 
      channel_->control_->consumer_seq.load(std::memory_order_acquire);
    fit = calc_batch_fit(my_ring, per, max_frames);
    if (fit == 0) {
      return std::unexpected(flow::FlowError::BackPressured);
    }
  }
  
  // 一次性分配 fit 个序列号（关键优化）
  const std::uint64_t base_seq = 
    channel_->control_->global_seq.fetch_add(fit, std::memory_order_relaxed);
  
  // 在私有 ring 上预留批量空间
  const std::uint64_t ring_pos = my_ring.producer_pos;
  const std::uint64_t span_bytes = static_cast<std::uint64_t>(per) * fit;
  my_ring.producer_pos += span_bytes;
  
  // 写入批量未提交帧头
  write_uncommitted_batch_headers(my_ring.ring, ring_pos, payload_len, 
                                   per, fit, base_seq);
  
  return flow::BatchClaim{
    .region = my_ring.ring.slice_mut(ring_pos, span_bytes),
    .start_pos = ring_pos,
    .frame_len = static_cast<std::uint32_t>(per),
    .frame_count = fit,
    .base_sequence = base_seq,  // 新增字段
  };
}
```

**Batch 优势**：
- 一次 `fetch_add(N)` 分配 N 个序列号，均摊 CAS 开销
- 在私有 ring 上连续写入，利用 magic ring 的零分片特性
- 对比当前架构：当前每条消息都要 CAS，batch 也救不了

### 2.5 Commit 算法

```cpp
void HybridMpscChannel::Tx::commit(const flow::Claim& claim) noexcept {
  auto& my_ring = channel_->producer_rings_[producer_id_];
  
  // 原子地标记为已提交（release 语义）
  const std::uint32_t committed_meta = 
    frame_meta(claim.start_pos, my_ring.ring.capacity(), claim.sequence, true);
  
  store_meta_release(my_ring.ring, claim.start_pos, committed_meta);
  
  // 可选：通知消费者（如果使用 futex 等待）
  if constexpr (requires { wait_.notify(); }) {
    channel_->control_->wait_word.fetch_add(1, std::memory_order_release);
    wait_.notify();
  }
}
```

### 2.6 Poll 算法（消费者）

```cpp
std::uint32_t HybridMpscChannel::Rx::poll(
    flow::MessageHandler auto handler, std::uint32_t limit) noexcept {
  
  std::uint32_t processed = 0;
  std::uint64_t expected_seq = 
    channel_->control_->consumer_seq.load(std::memory_order_relaxed);
  
  while (processed < limit) {
    // 步骤 1：从所有 producer ring 中找 seq == expected_seq 的消息
    std::optional<MessageView> msg = find_message_by_seq(expected_seq);
    
    if (!msg) {
      // 所有 ring 都扫描过了，没有找到 expected_seq
      break;
    }
    
    // 步骤 2：调用用户回调
    handler(msg->payload);
    
    // 步骤 3：更新消费者进度
    ++expected_seq;
    ++processed;
  }
  
  // 发布消费者进度（release 语义）
  if (processed > 0) {
    channel_->control_->consumer_seq.store(expected_seq, std::memory_order_release);
  }
  
  return processed;
}

// 从所有 ring 中查找指定序列号的消息
std::optional<MessageView> HybridMpscChannel::Rx::find_message_by_seq(
    std::uint64_t target_seq) noexcept {
  
  for (size_t i = 0; i < channel_->producer_rings_.size(); ++i) {
    auto& ring = channel_->producer_rings_[i];
    
    // 扫描该 ring 的 read cursor 附近
    auto msg = try_read_from_ring(ring, target_seq);
    if (msg) {
      return msg;
    }
  }
  
  return std::nullopt;
}

std::optional<MessageView> try_read_from_ring(
    ProducerRing& ring, std::uint64_t target_seq) noexcept {
  
  // 从当前 read position 开始扫描
  const std::uint64_t pos = ring.consumer_read_pos;
  
  // 读取帧头
  auto header_span = ring.ring.slice(pos, frame::kHeaderSize);
  const std::uint32_t len = read_u32(header_span.data());
  const std::uint32_t meta = 
    std::atomic_ref<std::uint32_t>(
      *reinterpret_cast<std::uint32_t*>(header_span.data() + 4))
      .load(std::memory_order_acquire);
  
  // 检查是否已提交
  if (!(meta & frame::FLAG_COMMITTED)) {
    return std::nullopt;  // 未提交，跳过此 ring
  }
  
  // 提取序列号（从 meta 中解码）
  const std::uint64_t seq = extract_sequence(meta);
  
  if (seq == target_seq) {
    // 找到目标消息
    auto payload = ring.ring.slice(pos + frame::kHeaderSize, len);
    ring.consumer_read_pos += frame::frame_len(len);  // 前进 read cursor
    return MessageView{seq, payload};
  }
  
  // 序列号不匹配，此 ring 当前没有目标消息
  return std::nullopt;
}
```

**Poll 算法要点**：
- 消费者维护 expected_seq，从所有 ring 中查找匹配的消息
- 每个 ring 维护独立的 `consumer_read_pos`
- 利用 magic ring 的连续性：跨边界读取无需特殊处理

**性能优化**：
- 可以缓存"上次找到消息的 ring id"，下次优先从该 ring 开始扫描
- 对于高吞吐场景，ring 扫描开销分摊到大批量消息上

---

## 3. 帧格式优化

### 3.1 新的 8 字节 header

```cpp
// 当前格式（继承自原设计）：
// [0:4) length
// [4:8) meta = (generation << 8) | flags

// 新格式（Hybrid 架构）：
// [0:4) length
// [4:8) meta = (sequence_low_24 << 8) | flags
//
// 完整 64 位序列号分两部分存储：
// - 低 24 位：存在 meta 字段
// - 高 40 位：存在 ring 的元数据区（每个 ring 一个）
```

**设计理由**：
- 24 位足够表示 16M 条消息的窗口（远大于 ring 容量）
- 高 40 位变化很慢（每 16M 条消息才递增 1），可以延迟加载
- 保持 8 字节 header，不增加开销

**当前实现备注（2026-07-08）**：
- 已实现 8 字节 header 中的 `sequence_low_24` 编解码。
- 高 40 位共享侧元数据尚未落地；为避免低 24 位回绕产生歧义，当前实现会拒绝“其他 producer ring 可容纳完整 24-bit sequence 周期”的配置。
- 默认 4MB per producer、16 个 named producer slot 内仍满足该约束；后续若需要更大 per-producer ring，应补齐共享高位元数据后移除该限制。

### 3.2 序列号编解码

```cpp
// 编码：将 64 位 seq 拆分存储
void encode_sequence(std::uint64_t seq, std::uint32_t& meta_inout, 
                     std::uint64_t& seq_high_out) noexcept {
  const std::uint32_t seq_low_24 = seq & 0x00FF'FFFFu;
  meta_inout = (seq_low_24 << 8) | (meta_inout & 0xFFu);  // 保留 flags
  seq_high_out = seq >> 24;
}

// 解码：从 meta + ring metadata 重建 64 位 seq
std::uint64_t decode_sequence(std::uint32_t meta, 
                               std::uint64_t seq_high) noexcept {
  const std::uint32_t seq_low_24 = (meta >> 8) & 0x00FF'FFFFu;
  return (seq_high << 24) | seq_low_24;
}
```

---

## 4. 跨进程支持

### 4.1 Named Hybrid Channel

```cpp
struct NamedHybridConfig {
  std::string name;  // 例如 "/salias_hybrid_test"
  std::uint32_t num_producers = 4;
  std::size_t ring_capacity_per_producer = 4 * 1024 * 1024;
  platform::HugePage huge = platform::HugePage::None;
};

// Creator 进程
auto channel = HybridMpscChannel::create_named(config);

// Joiner 进程（生产者）
auto tx = HybridMpscChannel::connect_producer(config.name, producer_id);

// Joiner 进程（消费者）
auto rx = HybridMpscChannel::connect_consumer(config.name);
```

### 4.2 共享内存布局

```
/dev/shm/salias_hybrid_<name>_control    (控制块)
/dev/shm/salias_hybrid_<name>_ring_0     (生产者 0 的 ring)
/dev/shm/salias_hybrid_<name>_ring_1     (生产者 1 的 ring)
...
/dev/shm/salias_hybrid_<name>_ring_N     (生产者 N-1 的 ring)
```

**初始化流程**：
1. Creator 创建所有共享内存文件
2. Creator 初始化控制块（`num_producers` 等）
3. Joiner 打开对应的 shm 文件并 mmap
4. 生产者只 mmap 自己的 ring + control
5. 消费者 mmap 所有 ring + control

---

## 5. 性能分析

### 5.1 理论加速比

**单生产者场景（1P/1C）**：

| 操作 | 当前架构 | Hybrid 架构 | 改进 |
|------|----------|-------------|------|
| CAS 竞争 | 无（但有 weak spurious failure） | 无 | — |
| 全局协调 | `compare_exchange_strong(tail, tail+len)` | `fetch_add(1)` | **10x 更快** |
| Ring 写入 | Magic ring（零抖动） | Magic ring（零抖动） | 持平 |
| Header | 8 字节 | 8 字节 | 持平 |

**预期**：1P/1C 提升 15-20%（主要来自 fetch_add 的速度优势）

**多生产者场景（4P/1C batch=1）**：

| 操作 | 当前架构 | Hybrid 架构 | 改进 |
|------|----------|-------------|------|
| CAS 竞争 | 4 个生产者竞争 1 个 `reserved_tail` | 4 个生产者各自独立 | **消除瓶颈** |
| CAS 成功率 | 平均 25%（1/4） | 100%（无竞争） | **4x** |
| Cache line bounce | 严重（单点热点） | 无（各自独立） | **消除** |
| Backoff 开销 | 平均 3 次重试 | 0 次重试 | **消除** |

**预期**：4P/1C 提升 3-4x（接近线性扩展）

**多生产者场景（4P/1C batch=16）**：

当前架构已经通过 batch 均摊 CAS，但仍有竞争：
- 当前：每 16 条消息 1 次 CAS，4 个生产者竞争
- Hybrid：每 16 条消息 1 次 fetch_add，4 个生产者独立

**预期**：4P/1C batch=16 提升 1.5-2x

### 5.2 预期吞吐量

基于 Tencent CVM 环境（4 vCPU, x86_64）：

| Scenario | Aeron IPC | salias 当前 | Hybrid 预期 | vs Aeron |
|----------|-----------|-------------|-------------|----------|
| 1P/1C batch=1 | 35 M/s | 26.6 M/s (0.76x) | **45 M/s** | **1.29x** |
| 4P/1C batch=1 | 16.5 M/s | 13.5 M/s (0.82x) | **55 M/s** | **3.33x** |
| 4P/1C batch=16 | ~25 M/s | 35 M/s (1.40x) | **90 M/s** | **3.60x** |

**为什么能超越 Aeron？**

1. **生产者无竞争**：
   - Aeron：每个 ExclusivePublication 独立，但仍需 Driver 协调流控
   - Hybrid：完全独立的 ring，只在分配 seq 时轻量同步

2. **Magic ring 零抖动**：
   - Aeron：term rotation 需要 64MB memset（p99.9 尖峰）
   - Hybrid：双映射消除 rotation，零抖动

3. **更小的 header**：
   - Aeron：32 字节 header + 32 字节对齐
   - Hybrid：8 字节 header + 8 字节对齐
   - 对 64B payload：有效载荷利用率从 66% 提升到 89%

4. **fetch_add vs compare_exchange**：
   - fetch_add(1) 比 compare_exchange_strong 快约 10x
   - fetch_add 永不失败，无 backoff 开销

### 5.3 延迟分析

**P50 延迟**：

| Component | 当前架构 | Hybrid | 改进 |
|-----------|----------|--------|------|
| Claim CAS | ~50ns (contended) | ~5ns (fetch_add) | **10x** |
| Ring write | ~20ns (cached) | ~20ns (cached) | 持平 |
| Commit | ~10ns (release store) | ~10ns (release store) | 持平 |
| **Total** | **~80ns** | **~35ns** | **2.3x** |

**P99.9 延迟**：

Aeron 的 term rotation 抖动：
- 每 64MB 触发一次清零（memset）
- 对 64B 消息：每 100 万条消息清零一次
- 清零耗时：~500us（64MB @ 128 GB/s）

Hybrid 无此抖动（magic ring 无需清零）。

### 5.4 内存效率

**Aeron**：
- 每个 stream：3 * 64MB = 192MB（3 个 term buffer）
- 4 个生产者：4 * 192MB = 768MB

**Hybrid**：
- 控制块：4KB
- 每个生产者 ring：4MB（可配置）
- 4 个生产者：4 * 4MB + 4KB ≈ 16MB

**内存节省**：768MB → 16MB（48x 更少）

---

## 6. 实现计划

### Phase 1：核心数据结构（1 周）

**新文件**：
- `src/core/channel/hybrid_mpsc.hpp`：HybridMpscChannel 类
- `src/core/channel/hybrid_control.hpp`：SharedControl 结构
- `src/core/frame/sequence.hpp`：序列号编解码

**任务**：
1. 定义 `ProducerRing` 结构
2. 定义 `SharedControl` 布局（128B 对齐）
3. 实现 `create()` 构造函数（创建 N 个独立 mapping）
4. 实现序列号编解码函数

**验收**：
- 编译通过
- 单元测试：创建 4P ring，验证 mapping 独立性

### Phase 2：Claim/Commit 实现（1 周）

**任务**：
1. 实现 `Tx::claim()` 单消息路径
2. 实现 `Tx::claim_batch()` 批量路径
3. 实现 `Tx::commit()` 和 `Tx::commit_batch()`
4. 更新 `flow::Claim` 结构（增加 `sequence` 字段）

**测试**：
- 单生产者写入 100 万条消息
- 验证序列号连续性
- 验证 ring 回绕正确性

### Phase 3：Poll 实现（1 周）

**任务**：
1. 实现 `Rx::poll()` 主循环
2. 实现 `find_message_by_seq()` 查找算法
3. 实现 `try_read_from_ring()` 单 ring 扫描
4. 优化：缓存上次命中的 ring id

**测试**：
- 1P/1C：验证消息顺序
- 4P/1C：验证多生产者消息交错正确性
- 验证 backpressure 行为

### Phase 4：Named 跨进程支持（1 周）

**任务**：
1. 实现 `create_named()` 创建命名 channel
2. 实现 `connect_producer()` 生产者连接
3. 实现 `connect_consumer()` 消费者连接
4. 共享内存生命周期管理

**测试**：
- Fork 测试：父进程创建，子进程连接
- 多进程测试：独立进程通过 shm 通信
- 异常测试：creator 崩溃后 joiner 清理

### Phase 5：性能测试与调优（1 周）

**任务**：
1. 更新 `tools/aeron_compare/salias_ipc_compare.cpp`
2. 增加 `--mode=hybrid` 参数
3. CPU pinning：每个生产者绑定独立核心
4. 采集 perf 数据：cache-misses, L1-dcache-loads

**目标**：
- 1P/1C：超越 Aeron 20%
- 4P/1C batch=1：超越 Aeron 3x
- 4P/1C batch=16：超越 Aeron 3.5x

### Phase 6：文档与测试覆盖（3 天）

**任务**：
1. 更新 API 文档（`doc/11-L7-api.md`）
2. 增加架构图（mermaid）
3. 性能对比报告（`doc/benchmarks/hybrid-vs-aeron.md`）
4. 补充并发测试用例

**交付物**：
- README 增加 Hybrid 示例
- Benchmark 报告
- 测试覆盖率 ≥ 85%

---

## 7. 风险与缓解

### 7.1 风险 1：Poll 性能 - Ring 扫描开销

**问题**：消费者需要扫描 N 个 ring 才能找到目标序列号的消息

**缓解**：
1. **优化 1**：缓存上次命中的 ring id，下次优先扫描
2. **优化 2**：每个 ring 维护 `min_seq` / `max_seq` 元数据，快速排除
3. **优化 3**：高吞吐时，扫描开销分摊到大批量消息上（64+ 条/poll）

**实测**：需要验证 4P 时的 poll 开销是否可接受

### 7.2 风险 2：内存占用 - N 个 ring

**问题**：每个生产者独立 ring，内存占用增加

**缓解**：
1. 默认 ring 大小降低到 4MB（vs 当前的 64MB）
2. Magic ring 无需 3x term 冗余，实际占用更少
3. 对比 Aeron：4P * 4MB = 16MB vs Aeron 4P * 192MB = 768MB

**结论**：总内存仍大幅少于 Aeron

### 7.3 风险 3：消息乱序 - Seq 分配与提交时间差

**问题**：生产者 A 先获得 seq=100，但生产者 B（seq=99）先 commit

**缓解**：
1. 这是预期行为：seq 保证**逻辑顺序**，commit 时间有微小差异
2. 消费者按 seq 有序读取，等待 seq=99 提交后才能读取 seq=100
3. 生产者 commit 延迟通常 < 1us，不会长时间阻塞

**结论**：可接受的设计权衡

### 7.4 风险 4：大规模生产者 - Seq 竞争重现

**问题**：32+ 生产者时，`fetch_add(1)` 仍可能成为瓶颈

**缓解**：
1. Fetch_add 的吞吐量 > 10 亿次/秒（远超 CAS）
2. 对 32P 场景，可以引入 **分段 sequencer**：
   - 8 个 seq generator，每个服务 4 个生产者
   - 消费者从 8 个 seq 空间合并读取

**结论**：32P 以内无需担心，更大规模需要架构扩展

---

## 8. 对比 Aeron 的优势总结

| 维度 | Aeron ExclusivePublication | Hybrid Ring | 优势 |
|------|----------------------------|-------------|------|
| **吞吐量** | 依赖 Driver 协调 | 完全独立 ring | ✅ 线性扩展 |
| **延迟** | Term rotation 抖动（p99.9） | Magic ring 零抖动 | ✅ 更平滑 |
| **内存** | 3 * 64MB per producer | 4MB per producer | ✅ 48x 更少 |
| **Header** | 32 字节 + 32 对齐 | 8 字节 + 8 对齐 | ✅ 4x 更高效 |
| **分片** | 需要 FragmentAssembler | 单帧连续 | ✅ 零重组 |
| **部署** | 需要 Media Driver | Driverless | ✅ 更简单 |
| **GC 抖动** | JVM（主实现） | C++23 无 GC | ✅ 更稳定 |

**核心突破**：
1. **Magic ring** 是 Aeron 无法复制的结构性优势
2. **Per-producer ring** 消除单点 CAS 瓶颈，实现线性扩展
3. **Lightweight sequencing** 比区间预留快 10 倍

---

## 9. 后续演进方向

### 9.1 MPMC 支持

扩展到多消费者：
- 每个消费者独立的 `consumer_seq`
- 生产者需要读取 `min(consumer_seq_0, ..., consumer_seq_M)`
- 可以复用当前的 MPMC 控制块设计

### 9.2 优先级队列

为不同生产者分配优先级：
- 高优先级生产者的消息优先 poll
- 实现：consumer 优先扫描高优先级 ring

### 9.3 动态扩容

运行时增加生产者：
- 预留 max_producers 个 ring slot
- 新生产者动态 mmap 新 ring
- 更新 control 的 `active_producers` 计数

### 9.4 Zero-copy Forwarding

跨 channel 转发：
- 消费者从 channel A 读取
- 直接在 channel B 的 ring 上 claim
- 零拷贝转发（只拷贝 seq 和指针）

---

## 10. 验收标准

### 10.1 正确性

- ✅ `ctest` 全部通过（包括 TSan/ASan）
- ✅ 多进程测试：4P/1C 发送 1000 万条消息，0 丢失
- ✅ 序列号连续性：消费者验证 seq 无跳跃
- ✅ Backpressure：满 ring 时 claim 返回 BackPressured

### 10.2 性能

- ✅ 1P/1C：≥ Aeron 的 1.2x（目标 1.29x）
- ✅ 4P/1C batch=1：≥ Aeron 的 3x（目标 3.33x）
- ✅ 4P/1C batch=16：≥ Aeron 的 3x（目标 3.60x）
- ✅ P99.9 延迟：无 term rotation 尖峰

### 10.3 可观测性

- ✅ 每个 ring 的统计信息（total_written, backpressure_count）
- ✅ 全局 seq 进度（监控用）
- ✅ 每个 ring 的内存占用

---

## 11. 实现检查清单

**Phase 1（核心结构）**：
- [ ] `hybrid_mpsc.hpp` 定义
- [ ] `SharedControl` 128B 对齐验证
- [ ] 创建 4P ring 单元测试
- [ ] 序列号编解码测试

**Phase 2（Claim/Commit）**：
- [ ] `claim()` 实现
- [ ] `claim_batch()` 实现
- [ ] `commit()` release 语义验证
- [ ] Backpressure 测试

**Phase 3（Poll）**：
- [ ] `poll()` 主循环
- [ ] `find_message_by_seq()` 实现
- [ ] 1P/1C 顺序验证
- [ ] 4P/1C 交错验证

**Phase 4（Named）**：
- [ ] `create_named()` 实现
- [ ] `connect_producer()` 实现
- [ ] Fork 测试
- [ ] 多进程测试

**Phase 5（性能）**：
- [ ] 更新 benchmark 工具
- [ ] CPU pinning 验证
- [ ] Perf 数据采集
- [ ] 达到目标吞吐量

**Phase 6（文档）**：
- [ ] API 文档更新
- [ ] 架构图
- [ ] Benchmark 报告
- [ ] 测试覆盖率报告

---

## 12. 总结

**Hybrid Ring 架构的本质**：
- 结合 per-producer ring 的**无竞争**
- 结合 magic ring 的**零抖动**
- 结合 lightweight sequencing 的**高吞吐**

**为什么能超越 Aeron**：
- Magic ring 是 Aeron 无法模仿的结构性优势（他们被网络传输绑定）
- Per-producer ring 消除单点瓶颈，实现线性扩展
- Fetch_add 比 CAS 区间预留快 10 倍

**预期结果**：
- 1P/1C：超越 Aeron 29%
- 4P/1C：超越 Aeron 3.3x
- 内存占用：降低 48x

**开始时间**：立即开始 Phase 1 实现
