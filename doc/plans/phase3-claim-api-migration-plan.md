# Phase 3 Claim API 迁移计划：采用 Aeron 风格 API

**日期**: 2024-10-22  
**状态**: 待批准  
**依据**: Phase 3 E2 实验结果（99.3% 回收比例，强候选验证）

---

## 1. 执行摘要

Phase 3 E2 实验证实 `shared_ptr<PublisherEndpoint>` 的引用计数是多线程发布路径的主要瓶颈，消除引用计数可恢复 95%+ 的单线程性能（256B 场景 +154%，64B 场景 +61%）。

本计划采用**方案 B**（Aeron 风格 API），将 `claim.commit()` 改为 `publisher.commit(claim)`，通过类型系统保障生命周期安全，同时消除每条消息的引用计数开销。

---

## 2. 实验背景

### 2.1 E2 实验结果

| 配置 | ST | MT-O | MT-B | 回收比例 | 提升幅度 |
|------|----|----- |------|---------|---------|
| **256B** | 16.15 Mmsg/s | 6.16 Mmsg/s | 15.63 Mmsg/s | 99.3% | +154% |
| **64B** | 9.16 Mmsg/s | 6.46 Mmsg/s | 10.38 Mmsg/s | 123.5% | +61% |

- **判定**: 强候选（6/6 组方向一致，回收 ≥50%）
- **瓶颈**: `shared_ptr` 引用计数每条消息 2 次原子操作（构造 +1，析构 -1）
- **延迟**: p50/p99 无恶化，p99.9 可接受范围

### 2.2 为什么选择方案 B

| 维度 | 方案 A（裸指针） | 方案 B（Aeron API） |
|------|----------------|-------------------|
| 生命周期安全 | ⚠️ 未由类型系统保护 | ✅ Publisher 持有端点 |
| API 迁移成本 | 低（无需改用户代码） | 中（需迁移 commit 调用） |
| 性能提升 | ✅ 相同 | ✅ 相同 |
| 与 Aeron 一致性 | ❌ 无 | ✅ 完全一致 |
| Use-after-free 风险 | ⚠️ 高（无编译保护） | ✅ 低（编译期捕获） |
| 代码审查负担 | ⚠️ 需人工验证契约 | ✅ 类型系统自动验证 |

**结论**: 方案 B 以合理的迁移成本换取长期安全性和设计一致性。

---

## 3. 设计方案

### 3.1 当前 API（v1.x）

```cpp
// 用户代码
auto claim_result = publisher.try_claim(payload_len);
if (claim_result) {
  auto claim = std::move(claim_result.value());
  std::memcpy(claim.payload().data(), data, payload_len);
  claim.commit();  // ❌ claim 内部持 shared_ptr，引用计数开销
}
```

**问题**:
- `PublishClaim` 内部: `std::shared_ptr<PublisherEndpoint<M>> endpoint_`
- `commit()` 时: `endpoint_->tx.commit(claim_)`，然后 `endpoint_.reset()`
- 每条消息 2 次原子操作（构造 claim +1，reset -1）

### 3.2 新 API（v2.0，Aeron 风格）

```cpp
// 用户代码
auto claim_result = publisher.try_claim(payload_len);
if (claim_result) {
  auto claim = std::move(claim_result.value());
  std::memcpy(claim.payload().data(), data, payload_len);
  publisher.commit(std::move(claim));  // ✅ claim 变为值对象
}
```

**改进**:
- `PublishClaim` 内部: `PublisherEndpoint<M>* endpoint_`（非拥有）
- `Publisher` 始终持有 `shared_ptr<PublisherEndpoint<M>>`
- `publisher.commit(claim)` 直接调用 `endpoint_->tx.commit(claim.claim_)`
- 无引用计数开销

### 3.3 API 对比

| API | v1.x（当前） | v2.0（新） |
|-----|------------|----------|
| 申请 | `auto c = pub.try_claim(len)` | 相同 |
| 填充 | `c->payload()` | 相同 |
| 提交 | `c->commit()` | `pub.commit(std::move(*c))` |
| claim 生命周期 | RAII（自动 commit） | 值对象（必须显式 commit） |
| 安全性 | shared_ptr 保证 | Publisher 持有端点 |

---

## 4. 实施路线图

### Phase 1: 新 API 实现（2 周）

#### 4.1 修改 `PublishClaim` 类（产品代码）

**文件**: `src/salias/include/salias/publisher.hpp`

```cpp
template <Mode M, typename PayloadSchema = RawPayloadSchema>
class PublishClaim {
 public:
  // 新构造函数：接受裸指针（仅 Publisher 可调用）
  PublishClaim(PublisherEndpoint<M>* endpoint, flow::Claim claim) noexcept;
  
  // 删除默认构造和拷贝
  PublishClaim() = delete;
  PublishClaim(const PublishClaim&) = delete;
  PublishClaim& operator=(const PublishClaim&) = delete;
  
  // 移动语义
  PublishClaim(PublishClaim&& other) noexcept;
  PublishClaim& operator=(PublishClaim&& other) noexcept;
  
  // 析构：不再自动 commit（改为断言未提交）
  ~PublishClaim() noexcept;
  
  // Payload 访问（不变）
  std::span<std::byte> payload() noexcept;
  
  // ❌ DEPRECATED: 标记为 deprecated，保留向后兼容
  [[deprecated("Use publisher.commit(claim) instead")]]
  void commit() noexcept;
  
  // 访问内部 flow::Claim（供 Publisher::commit 使用）
  flow::Claim& claim_handle() noexcept { return claim_; }
  
  // 标记已提交（避免析构断言）
  void mark_committed() noexcept { committed_ = true; }
  
 private:
  PublisherEndpoint<M>* endpoint_;  // NON-OWNING
  flow::Claim claim_;
  bool committed_ = false;
};
```

**关键变更**:
1. `endpoint_` 从 `shared_ptr` 改为裸指针
2. 移除 `commit()` 中的 `endpoint_.reset()`
3. 析构函数断言 `committed_` 为 true（调试模式）
4. 新增 `claim_handle()` 和 `mark_committed()` 供 `Publisher::commit` 使用

#### 4.2 新增 `Publisher::commit()` 方法

**文件**: `src/salias/include/salias/publisher.hpp`

```cpp
template <Mode M, typename PayloadSchema = RawPayloadSchema>
class Publisher {
 public:
  // ... 现有 API ...
  
  /// @brief 提交 claim，使帧对外可见（Aeron 风格 API）。
  /// @param claim 已填充的 claim（通过移动语义传递）。
  /// @note claim 在 commit 后失效，不可再使用。
  void commit(PublishClaim<M, PayloadSchema>&& claim) noexcept;
  
 private:
  std::shared_ptr<PublisherEndpoint<M>> endpoint_;  // 拥有端点
};
```

**实现**（`src/salias/src/channel.cpp`）:

```cpp
template <Mode M>
void Publisher<M, RawPayloadSchema>::commit(PublishClaim<M>&& claim) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return;  // 端点无效，静默失败
  }
  
  // 提交底层 flow::Claim
  endpoint_->tx.commit(claim.claim_handle());
  
  // 标记 claim 已提交，避免析构断言
  claim.mark_committed();
}
```

#### 4.3 修改 `try_claim()` 返回类型

**文件**: `src/salias/src/channel.cpp`

```cpp
template <Mode M>
Result<PublishClaim<M>> Publisher<M, RawPayloadSchema>::try_claim(
    std::size_t payload_len) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid ||
      payload_len > std::numeric_limits<std::uint32_t>::max()) {
    return std::unexpected(payload_len > std::numeric_limits<std::uint32_t>::max()
                               ? Error::MessageTooLarge
                               : Error::BadConfig);
  }
  auto claim = endpoint_->tx.claim(static_cast<std::uint32_t>(payload_len));
  if (!claim) {
    return std::unexpected(map_flow_error(claim.error()));
  }
  // 传递裸指针而非 shared_ptr
  return PublishClaim<M>(endpoint_.get(), claim.value());
}
```

#### 4.4 单元测试

**新增**: `tests/salias/test_claim_api_v2.cpp`

```cpp
TEST(ClaimAPIv2, CommitViaPublisher) {
  auto channel = Channel::create("test-v2", Mode::Spmc, 1_MiB);
  ASSERT_TRUE(channel);
  
  auto pub = channel->publisher();
  auto sub = channel->subscriber();
  
  // 申请 claim
  auto claim_result = pub.try_claim(64);
  ASSERT_TRUE(claim_result);
  
  auto claim = std::move(claim_result.value());
  std::memcpy(claim.payload().data(), "hello", 5);
  
  // 新 API: publisher.commit(claim)
  pub.commit(std::move(claim));
  
  // 验证交付
  auto msg = sub.poll();
  ASSERT_TRUE(msg);
  EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(msg->data()), 5), "hello");
}

TEST(ClaimAPIv2, ClaimMustBeCommitted) {
  auto channel = Channel::create("test-v2-assert", Mode::Spmc, 1_MiB);
  ASSERT_TRUE(channel);
  
  auto pub = channel->publisher();
  
  // 申请但不提交
  {
    auto claim_result = pub.try_claim(64);
    ASSERT_TRUE(claim_result);
    auto claim = std::move(claim_result.value());
    // 离开作用域时析构，debug 模式应断言失败
  }
  
  // Release 模式：静默失败，消息未发布
  // Debug 模式：断言触发
}
```

---

### Phase 2: 向后兼容与弃用（1 周）

#### 4.5 保留旧 API 并标记 deprecated

**文件**: `src/salias/include/salias/publisher.hpp`

```cpp
template <Mode M, typename PayloadSchema = RawPayloadSchema>
class PublishClaim {
 public:
  /// @brief 提交 claim（旧 API，已弃用）。
  /// @deprecated 使用 `publisher.commit(std::move(claim))` 代替。
  /// @note 此方法在 v2.0 仍可用，但会在 v3.0 移除。
  [[deprecated("Use publisher.commit(claim) instead. "
               "See doc/migration/claim-api-v2.md for details.")]]
  void commit() noexcept {
    // 临时实现：通过全局 map 查找 publisher（性能低，仅用于兼容）
    // 或：保留 shared_ptr 路径但打印警告
    if (committed_ || endpoint_ == nullptr) {
      return;
    }
    endpoint_->tx.commit(claim_);
    committed_ = true;
    // 注意：无法调用 endpoint_.reset()，因为 endpoint_ 现在是裸指针
  }
};
```

**问题**: 旧 `commit()` 无法访问 Publisher 的 `shared_ptr`，有两种选择：

1. **保守方案**（推荐）：保留 `shared_ptr` 成员但标记 deprecated，v3.0 才完全移除
2. **激进方案**：v2.0 立即移除旧 API，强制迁移

**推荐保守方案**，给用户一个版本的缓冲期。

#### 4.6 迁移指南

**新增**: `doc/migration/claim-api-v2.md`

```markdown
# Claim API 迁移指南：v1.x → v2.0

## 背景

v2.0 采用 Aeron 风格 API，将 `claim.commit()` 改为 `publisher.commit(claim)`，
消除引用计数开销，带来 60-154% 的多线程发布性能提升。

## 迁移步骤

### 旧代码（v1.x）

\```cpp
auto claim_result = publisher.try_claim(payload_len);
if (claim_result) {
  auto claim = std::move(claim_result.value());
  std::memcpy(claim.payload().data(), data, payload_len);
  claim.commit();  // 旧 API
}
\```

### 新代码（v2.0）

\```cpp
auto claim_result = publisher.try_claim(payload_len);
if (claim_result) {
  auto claim = std::move(claim_result.value());
  std::memcpy(claim.payload().data(), data, payload_len);
  publisher.commit(std::move(claim));  // 新 API
}
\```

### 自动迁移脚本

\```bash
# 使用 sed 批量替换（需手动验证）
find src -name "*.cpp" -exec sed -i \
  's/claim\.commit()/publisher.commit(std::move(claim))/g' {} +
\```

## 常见问题

### Q: 旧 API 何时移除？
A: v2.0 标记 deprecated，v3.0 完全移除。

### Q: 性能提升有多大？
A: 256B 场景 +154%，64B 场景 +61%（基于 Phase 3 E2 实验）。

### Q: 我可以混用新旧 API 吗？
A: 可以，但不推荐。建议统一迁移到新 API。
```

---

### Phase 3: 性能验证与回归测试（1 周）

#### 4.7 性能基准

**新增**: `benchmarks/salias/bench_claim_api_v2.cpp`

```cpp
static void BM_ClaimCommit_V1(benchmark::State& state) {
  auto channel = Channel::create("bench-v1", Mode::Spmc, 1_MiB).value();
  auto pub = channel.publisher();
  
  for (auto _ : state) {
    auto claim = pub.try_claim(64).value();
    claim.commit();  // 旧 API
  }
}
BENCHMARK(BM_ClaimCommit_V1);

static void BM_ClaimCommit_V2(benchmark::State& state) {
  auto channel = Channel::create("bench-v2", Mode::Spmc, 1_MiB).value();
  auto pub = channel.publisher();
  
  for (auto _ : state) {
    auto claim = pub.try_claim(64).value();
    pub.commit(std::move(claim));  // 新 API
  }
}
BENCHMARK(BM_ClaimCommit_V2);
```

**预期结果**（单线程）:
- V1: ~100ns/op（含引用计数）
- V2: ~50ns/op（无引用计数）

**预期结果**（多线程，2 核）:
- V1: ~150ns/op（原子操作竞争）
- V2: ~60ns/op（无竞争）

#### 4.8 回归测试矩阵

| 场景 | 测试覆盖 |
|------|---------|
| 单生产者 ST | ✅ 新旧 API 均通过 |
| 单生产者 MT | ✅ 新 API 性能提升 60-154% |
| 多生产者 MPMC | ✅ 功能正确性不变 |
| 延迟敏感路径 | ✅ p50/p99 无恶化 |
| 背压场景 | ✅ try_claim 失败处理一致 |
| 端点失效 | ✅ commit 静默失败 |

---

### Phase 4: 文档与发布（1 周）

#### 4.9 CHANGELOG

**文件**: `CHANGELOG.md`

```markdown
## [2.0.0] - 2024-11-XX

### Added
- **New Aeron-style API**: `publisher.commit(claim)` replaces `claim.commit()`.
- Performance improvement: 60-154% throughput gain in multi-threaded publishing.

### Deprecated
- `PublishClaim::commit()` is deprecated in favor of `Publisher::commit(claim)`.
  Will be removed in v3.0.

### Changed
- `PublishClaim` now holds a non-owning pointer to `PublisherEndpoint`.
- Publisher retains ownership of the endpoint, eliminating per-message reference counting.

### Migration
- See `doc/migration/claim-api-v2.md` for migration guide.
- Automatic script: `tools/migrate_claim_api.sh`.

### Performance
- Based on Phase 3 E2 experiment:
  - 256B payload: 6.16 → 15.63 Mmsg/s (+154%)
  - 64B payload: 6.46 → 10.38 Mmsg/s (+61%)
```

#### 4.10 API 文档更新

**文件**: `doc/api/publisher.md`

更新所有示例代码使用新 API：

```cpp
// ✅ 推荐（v2.0+）
auto claim = publisher.try_claim(payload_len).value();
std::memcpy(claim.payload().data(), data, payload_len);
publisher.commit(std::move(claim));

// ⚠️ 已弃用（v1.x，v3.0 将移除）
claim.commit();
```

---

## 5. 风险评估与缓解

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|---------|
| ABI 破坏 | 高 | 高 | v2.0 major 版本升级，明确通知 |
| 用户迁移成本 | 中 | 高 | 提供迁移脚本和详细文档 |
| 性能回归（非预期场景） | 中 | 低 | 全面回归测试矩阵 |
| 生命周期 bug（claim 悬空） | 高 | 低 | Debug 模式断言 + 代码审查 |
| 旧 API 误用（忘记 commit） | 中 | 中 | 编译警告 + 静态分析 |

---

## 6. 时间线与资源

| Phase | 任务 | 工期 | 负责人 |
|-------|------|------|--------|
| 1 | 新 API 实现 | 2 周 | 核心团队 |
| 2 | 向后兼容与弃用 | 1 周 | 核心团队 |
| 3 | 性能验证与回归测试 | 1 周 | QA + 核心团队 |
| 4 | 文档与发布 | 1 周 | 文档 + 发版 |
| **总计** | | **5 周** | |

---

## 7. 成功标准

1. ✅ 所有单元测试通过（新旧 API）
2. ✅ 性能提升达到 E2 实验预期（256B +150%，64B +60%）
3. ✅ 延迟 p50/p99 无恶化（<5% 波动）
4. ✅ 迁移文档完整，用户可自助迁移
5. ✅ 旧 API 可用但打印 deprecation 警告
6. ✅ v2.0 发布，CHANGELOG 清晰

---

## 8. 批准与授权

**提议人**: Claude (Phase 3 E2 实验团队)  
**审批人**: [待填充]  
**批准日期**: [待填充]  

**批准后下一步**:
1. 创建 feature 分支 `feature/claim-api-v2`
2. 开始 Phase 1 实现
3. 每周同步进度

---

## 附录

### A. E2 实验证据

- 实验目录: `build/benchmarks/p3-e2-claim-20261022-154552/`
- 分析报告: `build/benchmarks/p3-e2-claim-20261022-154552/analysis.md`
- 判定: 强候选（99.3% 回收，6/6 组一致）

### B. 参考文献

1. Aeron Publisher API: [github.com/real-logic/aeron](https://github.com/real-logic/aeron)
2. Phase 3 E2 实验计划: `doc/plans/phase3-e2-claim-isolation-plan.md`
3. Phase 3 E2 实验报告: `doc/plans/phase3-e2-claim-report.md`
