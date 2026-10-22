# Phase 3 Claim API 重构计划：v4.0.0 破坏性变更

**日期**: 2026-10-22  
**状态**: 已随 v4.0.0 发布（2026-10-22）。实现、内部迁移、文档与工具已完成；两次正式产品 A/B 的收益方向稳定，64 B claim API 吞吐 GO 未复现；延迟长尾与整体性能验收仍未通过
**版本**: v3.0.0 → v4.0.0（破坏性 ABI 变更）  
**依据**: Phase 3 E2 实验结果（99.3% 回收比例，强候选验证）

---

## 1. 执行摘要

Phase 3 E2 实验证实 `shared_ptr<PublisherEndpoint>` 的引用计数是多线程发布路径的主要瓶颈。本计划通过 v4.0.0 破坏性变更，将 claim 生命周期模型从"自拥有端点"改为"Publisher 拥有端点，claim 仅持有位置信息"，消除每条消息的引用计数开销。

**核心变更**:
- `PublishClaim` 不再持有 `shared_ptr<PublisherEndpoint>`，仅保存 `flow::Claim` 和非拥有发布端身份
- 移除 `claim.commit()`，改为 `publisher.commit(std::move(claim))`
- Publisher 校验 claim 归属，防止跨 Publisher 提交

**预期收益**（基于 E2 诊断实验）:
- E2 实验显示消除引用计数可回收 95%+ 的多线程性能损失
- 产品代码需通过同二进制、同拓扑 A/B 基准独立验证
- p99 尾部延迟有 ~30% 成本（E2 数据：3759 → 4959 ns），p50/p99.9 可接受

---

## 2. 实验背景

### 2.1 E2 实验结果

| 配置 | ST | MT-O | MT-B | 回收比例 |
|------|----|----- |------|---------|
| **256B** | 16.15 Mmsg/s | 6.16 Mmsg/s | 15.63 Mmsg/s | 99.3% |
| **64B** | 9.16 Mmsg/s | 6.46 Mmsg/s | 10.38 Mmsg/s | 123.5% |

- **判定**: 强候选（6/6 组方向一致，回收 ≥50%）
- **瓶颈**: `shared_ptr` 引用计数每条消息 2 次原子操作（构造 +1，析构 -1）
- **延迟影响**: p50/p99.9 可接受，p99 恶化 ~30%（3759 → 4959 ns）

**重要说明**: E2 使用诊断探针（借用指针），产品代码性能需独立验证。

### 2.2 为什么是 v4.0.0 破坏性变更

| 维度 | 向后兼容 | v4.0.0 破坏性变更 |
|------|---------|------------------|
| `claim.commit()` | 保留但性能无改善 | ❌ 移除 |
| 引用计数 | 仍存在 | ✅ 完全消除 |
| 生命周期安全 | shared_ptr 保证 | Publisher 拥有端点 |
| 跨 Publisher 提交 | 无校验 | ✅ 运行时校验拒绝 |
| 用户迁移成本 | 低 | 中（需改 commit 调用） |

**结论**: 只有破坏性变更才能彻底消除引用计数，兼容方案无法达到目标。

---

## 3. 设计方案

### 3.1 当前 API（v3.0.0）

```cpp
// 用户代码
auto claim_result = publisher.try_claim(payload_len);
if (claim_result) {
  auto claim = std::move(claim_result.value());
  std::memcpy(claim.payload().data(), data, payload_len);
  claim.commit();  // ❌ claim 内部持 shared_ptr，引用计数开销
}
```

**当前实现**:
```cpp
template <Mode M>
class PublishClaim<M, RawPayloadSchema> {
 private:
  std::shared_ptr<PublisherEndpoint<M>> endpoint_;  // 每条消息 +1/-1 引用计数
  flow::Claim claim_;
  bool committed_;
};
```

**问题**:
1. 每条消息 2 次原子操作（构造 +1，析构/reset -1）
2. 多线程竞争 shared_ptr 控制块
3. 无法校验跨 Publisher 提交（pubA 申请，pubB 提交）

### 3.2 新 API（v4.0.0）

```cpp
// 用户代码
auto claim_result = publisher.try_claim(payload_len);
if (claim_result) {
  auto claim = std::move(claim_result.value());
  std::memcpy(claim.payload().data(), data, payload_len);
  publisher.commit(std::move(claim));  // ✅ claim 仅持有位置信息
}
```

**新实现**:
```cpp
template <Mode M>
class PublishClaim<M, RawPayloadSchema> {
 public:
  PublishClaim(const PublisherEndpoint<M>* owner, flow::Claim claim) noexcept
      : owner_(owner), claim_(claim), committed_(false) {}
  
  std::span<std::byte> payload() noexcept { return claim_.payload; }
  
  // ❌ commit() 已移除
  
  // 内部访问（供 Publisher::commit 使用）
  flow::Claim& claim_handle() noexcept { return claim_; }
  const PublisherEndpoint<M>* owner() const noexcept { return owner_; }
  void mark_committed() noexcept { committed_ = true; }
  
  ~PublishClaim() noexcept {
    // Debug 模式断言已提交
    assert(committed_ && "PublishClaim destroyed without commit");
  }
  
 private:
  const PublisherEndpoint<M>* owner_ = nullptr;  // 非拥有身份令牌（8 字节）
  flow::Claim claim_{};                          // 实测 56 字节（x86-64）
  bool committed_ = true;                        // Debug 断言（1 字节，有填充）
  // 实测 sizeof(PublishClaim<FifoMpsc>)：v3 为 80，v4 为 72。
};
```

**关键改进**:
1. ✅ **零引用计数**: claim 不持有 shared_ptr
2. ✅ **Publisher 校验**: 通过非拥有端点身份防止跨 Publisher 提交
3. ✅ **生命周期清晰**: Publisher 持有端点，commit 使用自身端点
4. ✅ **内存占用减少**: 实测 claim 对象由 80 字节降至 72 字节

### 3.3 Publisher::commit() 实现

```cpp
template <Mode M>
class Publisher<M, RawPayloadSchema> {
 public:
  /// @brief 提交 claim，使帧对外可见。
  /// @param claim 已填充的 claim（通过移动语义传递）。
  /// @pre claim 必须由此 Publisher 申请。
  /// @note claim 在 commit 后失效，不可再使用。
  void commit(PublishClaim<M>&& claim) noexcept {
    // 校验 claim 属于此 Publisher
    if (claim.owner() != endpoint_.get()) {
      // 拒绝：claim 来自其他 Publisher
      // Debug 模式断言，Release 静默失败
      assert(false && "Claim does not belong to this Publisher");
      return;
    }
    
    if (endpoint_ == nullptr || !endpoint_->valid) {
      return;  // 端点无效，静默失败
    }
    
    // 使用自身端点提交
    endpoint_->tx.commit(claim.claim_handle());
    
    // 标记 claim 已提交，避免析构断言
    claim.mark_committed();
  }
  
  Result<PublishClaim<M>> try_claim(std::size_t payload_len) noexcept {
    if (endpoint_ == nullptr || !endpoint_->valid ||
        payload_len > std::numeric_limits<std::uint32_t>::max()) {
      return std::unexpected(/* ... */);
    }
    auto claim = endpoint_->tx.claim(static_cast<std::uint32_t>(payload_len));
    if (!claim) {
      return std::unexpected(map_flow_error(claim.error()));
    }
    // 传递位置信息和 publisher 标识
      return PublishClaim<M>(endpoint_.get(), claim.value());
  }
  
 private:
  std::shared_ptr<PublisherEndpoint<M>> endpoint_;  // Publisher 拥有端点
};
```

### 3.4 Schema-bound 接口同步

```cpp
template <Mode M, detail::WireSchema Schema>
class PublishClaim<M, Schema> {
 public:
  using T = typename Schema::message_type;
  
  std::span<std::byte> payload() noexcept { return claim_.payload(); }
  
  // ❌ commit() 已移除，与 RawPayloadSchema 保持一致
  
 private:
  explicit PublishClaim(PublishClaim<M, RawPayloadSchema> claim) noexcept
      : claim_(std::move(claim)) {}
  
  PublishClaim<M, RawPayloadSchema> claim_;  // 委托到 raw claim
  
  template <Mode, class>
  friend class Publisher;
};

template <Mode M, detail::WireSchema Schema>
class Publisher<M, Schema> {
 public:
  using T = typename Schema::message_type;
  
  // 新增 commit() 方法
  void commit(PublishClaim<M, Schema>&& claim) noexcept {
    publisher_.commit(std::move(claim.claim_));  // 委托到 raw publisher
  }
  
  Result<PublishClaim<M, Schema>> try_claim() noexcept {
    auto claim = publisher_.try_claim(sizeof(T));
    if (!claim) {
      return std::unexpected(claim.error());
    }
    return PublishClaim<M, Schema>(std::move(claim.value()));
  }
  
 private:
  Publisher<M, RawPayloadSchema> publisher_;
};
```

---

## 4. 实施路线图

### Phase 1: 核心实现与测试（3 周）

**完成状态（2026-10-22）**：以下 4.1–4.5 已实施，新增
`test/api/claim_api_v4_test.cpp` 并并入 `salias_api_tests`。实际实现使用非拥有
`const PublisherEndpoint<M>* owner_` 作为身份令牌，而不是计划初稿中的整数
`publisher_id_`；claim 对象实测 72 字节（v3 为 80 字节）。

#### 4.1 修改 PublishClaim 类

**文件**: `src/salias/include/salias/publisher.hpp`

- 移除 `shared_ptr<PublisherEndpoint>` 成员
- 添加非拥有 `const PublisherEndpoint<M>* owner_` 身份成员
- 移除 `commit()` 方法
- 添加 `claim_handle()`, `owner()`, `mark_committed()` 访问器
- 修改析构函数断言逻辑

#### 4.2 新增 Publisher::commit() 方法

**文件**: `src/salias/include/salias/publisher.hpp`

- 添加 `void commit(PublishClaim<M, PayloadSchema>&& claim)` 方法
- 比较 claim 保存的非拥有身份与 `endpoint_.get()`，不访问其他发布端
- 委托到 `endpoint_->tx.commit(claim.claim_)`

#### 4.3 修改 try_claim() 返回值构造

**文件**: `src/salias/src/channel.cpp`

- 修改 `return PublishClaim<M>(...)` 传递 `endpoint_.get()`

#### 4.4 同步 Schema-bound 接口

**文件**: `src/salias/include/salias/publisher.hpp`

- `PublishClaim<M, Schema>` 移除 `commit()`
- `Publisher<M, Schema>` 添加 `commit(claim)`

#### 4.5 单元测试

**实际新增**: `test/api/claim_api_v4_test.cpp`（并入 `salias_api_tests`）

实际用例使用仓库的 `FifoMpscChannel::create(Config)` 和
`channel.publisher()` / `channel.subscriber()` 创建端点，覆盖 raw/schema-bound
提交、移动后提交、同一 Publisher 副本提交、跨 Publisher 拒绝、重复提交与
moved-from claim。没有旧草稿里的 `ChannelConfig::spmc` 或 `poll()` 返回 vector API。

---

### Phase 2: 内部迁移与编译修复（2 周）

**完成状态（2026-10-22）**：示例、API/通道/mcpp 测试、Aeron 对比器、HFT 验收和
final bench 已全部迁移；仓库自有调用点已无 `claim.commit()` / `claim->commit()`。

#### 4.6 迁移内部调用点

**实际统计**: 迁移前仓库内部有 30 余处 `claim.commit()` / `claim->commit()`，涵盖示例、API/通道/mcpp 测试、Aeron 对比器、HFT 验收与 final bench；计划最初写“0”有误。

**迁移策略**: 逐点人工迁移。提交用的 Publisher 变量名不固定，`try_claim()` 也可能
返回 `Result` 而不是 claim；盲目用 `sed` 替换会改错对象或破坏错误处理，因此没有提供
会伪装成“自动安全迁移”的脚本。下游先用迁移指南中的只读扫描定位，再按实际变量名替换。

#### 4.7 mcpp 集成测试迁移

**文件**: `tools/aeron_compare/salias_ipc_compare.cpp`（及其他集成测试）

- 手动迁移所有 claim 使用点
- 验证测试通过

---

### Phase 3: 性能验证（2 周）

**完成状态（2026-10-22）**：三臂构建、吞吐/延迟 runner 与独立分析器已完成两次完整
正式矩阵。第一次证据目录为 `build/benchmarks/claim-api-v4-20261022-181707/`，第二次为
`build/benchmarks/claim-api-v4-release-20261022-204139/`；每次均含 36 个吞吐 block、
4 个延迟 case（每个 case 含 B/S/A 三臂各 10 轮正式轮次）。

两次正式结果：

| 运行 | 证据目录 | 64 B S/B | 256 B S/B | 64 B 判定 |
|---|---|---:|---:|---|
| 第一次 | `claim-api-v4-20261022-181707` | 1.258804 (+25.88%) | 1.281718 (+28.17%) | GO |
| 第二次 | `claim-api-v4-release-20261022-204139` | 1.238393 (+23.84%) | 1.290653 (+29.07%) | INCONCLUSIVE |

第二次运行的三段对比：

| Payload | 对比 | 候选 Mmsg/s | 基线 Mmsg/s | 相对变化 | 组方向 | 精确置换 p |
|---|---:|---:|---:|---:|---:|---:|
| 64 B | S/B claim API | 6.139 | 4.957 | **+23.84%** | 6/6 | 0.002165 |
| 64 B | A/S fence | 7.044 | 6.139 | +14.74% | 6/6 | 0.002165 |
| 64 B | A/B combined | 7.044 | 4.957 | +42.10% | 6/6 | 0.002165 |
| 256 B | S/B claim API | 6.294 | 4.877 | **+29.07%** | 6/6 | 0.002165 |
| 256 B | A/S fence | 7.038 | 6.294 | +11.82% | 6/6 | 0.002165 |
| 256 B | A/B combined | 7.038 | 4.877 | +44.32% | 6/6 | 0.002165 |

第一次 64 B S/B 中位数比 **1.258804**，达到预注册 E1 的 1.25 门槛，精确置换检验
**p=0.002165**（2/924），6/6 组方向一致，判为 **GO**。第二次为 **1.238393**，方向和
显著性仍通过（p=0.002165、6/6），但比值低于 1.25，分析器判为 **INCONCLUSIVE**。
预注册指标是 120 轮池化中位数；第二次组中位数比为 1.258986，但不能事后改用这个
统计量，也没有登记“不确定后扩样”分支。因此结论是**收益方向稳定存在，但 claim API
吞吐 GO 尚未复现**，不能宣称已通过吞吐门槛。256 B 的 +29.07% 只作描述性报告，
不能套用 64 B 门槛。A/S 与 A/B 是同一 v4 二进制的伴随结果，不改变 P3-1 的 192 B
与 HFT 长尾结论。

第二次 1P1S、1M msg/s 短延迟（各臂 10 轮独立分位数取中位数，不合并直方图）：

| 对比 | 口径 | p50 候选/基线 (ns) | p99 候选/基线 (ns) | p99.9 候选/基线 (ns) |
|---|---|---:|---:|---:|
| 64 B S/B | attempt | 385/387 | 471/483 | 4287/4287 |
| 64 B S/B | planned | 447/439 | 3375/4079 | 12351/12799 |
| 64 B A/S | attempt | 381/385 | 487/471 | 8959/4287 |
| 256 B S/B | attempt | 389/377 | 501/513 | 4607/6047 |
| 256 B S/B | planned | 443/431 | 4639/4543 | 14079/14143 |
| 256 B A/S | attempt | 381/389 | 1247/501 | 9471/4607 |

轮间范围仍有毫秒级尖峰（64 B planned p99.9 的候选/基线轮间范围为
7743–2064383 / 9983–23039 ns；256 B A/S attempt p99.9 为 751–14207 / 4287–9727 ns）。
本轮没有重跑 HFT 长尾门禁，因此**不能宣称延迟非劣、长尾已解决或 v4.0.0 整体发布
验收通过**；原定“完整的尝试/计划延迟矩阵已执行”与“延迟非劣已验收”是不同命题。
完整轮间范围、S/B、A/S、A/B 三段对比和原始校验见两次证据目录中的
`claim-api-v4-64b.md`、`claim-api-v4-256b.md` 与 `claim-api-v4-analysis.json`。

#### 4.8 性能基准（同二进制 A/B）

**实现**: Google Benchmark 的单进程线程组不能替代产品 IPC A/B。已改为编译同一份
benchmark 客户端源码的三臂对照：

- `B`：`git:v3.0.0` 的产品头文件和实现，客户端使用 `claim.commit()`。
- `S`：工作区 v4 产品，设置 `SALIAS_DISABLE_ASYMMETRIC_FENCE=1`，客户端使用
  `publisher.commit(std::move(claim))`。
- `A`：同一 v4 产品与客户端，默认非对称屏障。

`tools/aeron_compare/build_claim_v4_ab.py` 从当前 benchmark 客户端模板生成 B/S 两个
变体：两者只差两处提交调用，并分别记录实际生成源码的 SHA-256；v3 产品源码固定到
`git:v3.0.0`，同时单独记录模板与基线源码身份。`doc/plans/runners/p3-claim-api-v4-ab.py` 执行正式矩阵，
`tools/aeron_compare/analyze_claim_v4_ab.py` 验证身份、环境、组完整性、原始 RESULT
与校验和，分别输出 S/B、A/S、A/B。

```bash
python3 tools/aeron_compare/build_claim_v4_ab.py \
  --output build/claim-api-v4-ab-$(date +%Y%m%d-%H%M%S)

systemd-run --user --wait --pipe --collect \
  -p "WorkingDirectory=$PWD" -p 'AllowedCPUs=0-3' -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
  -p TasksMax=64 -p RuntimeMaxSec=14400s -p KillMode=control-group \
  python3 doc/plans/runners/p3-claim-api-v4-ab.py \
  --build-manifest build/claim-api-v4-ab-<timestamp>/manifest.json \
  --output build/benchmarks/claim-api-v4-<timestamp>
```

正式矩阵为 64/256 B，每格 6 组 × 20 轮、每轮 50M 消息；延迟为 1P1S、1M msg/s，
attempt/planned 各 10 轮 × 2 秒。吞吐 runner 的两个 payload 共 36 个 block 按组反转臂顺序，
每轮独立 systemd cgroup。延迟 runner 每轮按 B/S/A 轮换。

**验收标准与判定**:
- claim API 结论只采用 S/B；A/S 单独归因屏障；A/B 只报告组合效果。
- 64 B 吞吐以预注册 E1 规则判定：中位数比 ≥1.25、精确置换 p<0.01 且至少 5/6 组方向一致为 GO。
  第一次正式运行判为 GO（1.258804），第二次为 INCONCLUSIVE（1.238393）；**GO 未复现**。
- 256 B 与延迟按正式证据报告；p50/p99/p99.9 使用各轮分位数的中位数，不合并直方图。
- 延迟不在此处临时设门限：正式结果先完整报告 S/B、A/S、A/B 的 p50/p99/p99.9 及轮间范围，
  再结合既有 p99/p99.9 5% 非劣规则评审；不能事后挑选更有利的门槛。

#### 4.9 回归测试矩阵

| 场景 | 测试覆盖 |
|------|---------|
| 单生产者 SPMC | ✅ 功能正确性 |
| 多生产者 MPMC | ✅ 功能正确性 |
| 背压场景 | ✅ try_claim 失败处理 |
| 跨 Publisher 提交 | ✅ 运行时拒绝 |
| 端点失效 | ✅ commit 静默失败 |
| Schema-bound API | ✅ 与 raw API 行为一致 |

---

### Phase 4: 文档与发布（1 周）

#### 4.10 CHANGELOG

**实际更新**: `CHANGELOG.md` 的 `[Unreleased]` 已记录破坏性变更、测试与迁移说明；
正式发版时按实际发布内容整理为 v4.0.0 小节，不保留下面的历史草稿日期。

```markdown
## [4.0.0] - YYYY-MM-DD

### ⚠️ BREAKING CHANGES

- **Removed `PublishClaim::commit()`**: Use `Publisher::commit(claim)` instead.
- **Claim lifecycle model changed**: `PublishClaim` no longer holds a `shared_ptr` 
  to the endpoint. Publisher retains ownership.
- **Cross-publisher commit validation**: Runtime check prevents committing a claim 
  through a different publisher than the one that issued it.

### Added

- `Publisher::commit(PublishClaim&&)`: New API to commit claims.
- Runtime validation: Rejects cross-publisher commits (debug: assert, release: silent failure).

### Changed

- `PublishClaim` internal: Now holds `flow::Claim` + a non-owning publisher identity instead of `shared_ptr<PublisherEndpoint>`.
- Performance: Eliminated per-message reference counting in multi-threaded publishing.

### Migration

See `doc/migration/claim-api-v4.md` for migration guide.

**Migration check**:
```bash
rg -n 'claim(\.|->)commit\(' --glob '!doc/**' --glob '!build/**'
```

### Performance

- Phase 3 E2 experiment showed 95%+ recovery of single-threaded performance 
  when eliminating reference counting (diagnostic probe, not product code).
- Product A/B on 2026-10-22 measured 64 B S/B +25.88%/+23.84% and 256 B S/B
  +28.17%/+29.07% in two formal 1P2S broadcast runs. The first 64 B run passed
  the pre-registered E1 GO rule at 1.258804; the second was 1.238393 and
  INCONCLUSIVE. The GO did not reproduce, so the throughput gate is not claimed.
- The 1P1S short latency matrix is not a long-tail acceptance: p99/p99.9 still
  show millisecond-scale round-to-round spikes. HFT non-inferiority was not rerun.

### Removed

- `PublishClaim::commit()` (use `Publisher::commit(claim)` instead).
```

#### 4.11 迁移指南

**新增**: `doc/migration/claim-api-v4.md`

```markdown
# Claim API 迁移指南：v3.0.0 → v4.0.0

## 破坏性变更

v4.0.0 移除了 `PublishClaim::commit()`，改为 `Publisher::commit(claim)`，
以消除引用计数开销。这是破坏性 ABI 变更。

## 迁移步骤

### 旧代码（v3.0.0）

\```cpp
auto claim_result = publisher.try_claim(payload_len);
if (claim_result) {
  auto claim = std::move(claim_result.value());
  std::memcpy(claim.payload().data(), data, payload_len);
  claim.commit();  // ❌ v4.0.0 已移除
}
\```

### 新代码（v4.0.0）

\```cpp
auto claim_result = publisher.try_claim(payload_len);
if (claim_result) {
  auto claim = std::move(claim_result.value());
  std::memcpy(claim.payload().data(), data, payload_len);
  publisher.commit(std::move(claim));  // ✅ v4.0.0 新 API
}
\```

### 升级检查

提交用的 Publisher 变量名不固定，仓库不提供盲目 `sed` 脚本。先执行：

\```bash
rg -n 'claim(\.|->)commit\(' --glob '!doc/**' --glob '!build/**'
\```

逐个确认 claim 的来源 Publisher，再改为 `publisher.commit(std::move(claim))` 或
`publisher.commit(std::move(*claim))`。

## 新特性

### Publisher 校验

v4.0.0 运行时检查 claim 归属：

\```cpp
auto pub_a = channel.publisher<uint64_t>();
auto pub_b = channel.publisher<uint64_t>();

auto claim = std::move(pub_a.try_claim(64).value());
pub_b.commit(std::move(claim));  // ❌ 拒绝：claim 不属于 pub_b
\```

- **Debug 模式**: 断言失败
- **Release 模式**: 静默失败，消息未发布

### 生命周期

claim 必须显式 commit，否则 Debug 模式析构时断言：

\```cpp
{
  auto claim = std::move(pub.try_claim(64).value());
  // 忘记 commit
}  // Debug: 断言失败 "PublishClaim destroyed without commit"
\```

## 性能影响

- **1P2S 吞吐**: 两次正式 S/B 中位数比分别为 64 B **1.258804 / 1.238393**、
  256 B **1.281718 / 1.290653**；第一次 64 B 达到预注册 E1 GO 门槛，第二次未达到，
  因此 GO 未复现，256 B 为描述性结果。
- **1P1S 短延迟**: 第二次 64 B planned p50 为 447/439 ns（候选/基线），p99 为
  3375/4079 ns；256 B planned p50 为 443/431 ns。轮间仍有毫秒级尖峰，
  不能据此声称长尾非劣。
- 上述数字只适用于本机 4 vCPU、1 MiB ring、batch=1、poll-limit=64 的条件；
  不是生产 SLA，也不替代 HFT 完整性/长尾门禁。

## 常见问题

### Q: 为什么是 v4.0.0？
A: 破坏性 ABI 变更，移除了 `claim.commit()`。

### Q: 向后兼容吗？
A: 否，必须修改所有 `claim.commit()` 调用点。

### Q: 我可以延迟升级吗？
A: 可以继续使用已发布的 v3.0.0；升级窗口与维护承诺以实际发布说明为准，本计划不预设期限。
```

#### 4.12 API 文档更新

**实际文件**: `doc/11-L7-api.md` 与 `doc/migration/claim-api-v4.md`

更新所有示例代码使用新 API，标注旧 API 已移除。

---

## 5. 风险评估与缓解

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|---------|
| ABI 破坏 | 高 | 确定 | v4.0.0 major 升级，提前 3 个月通知 |
| 用户迁移失败 | 中 | 中 | 提供逐点迁移指南与只读扫描命令，不提供可能改错对象的自动脚本 |
| 性能不达预期 | 中 | 低 | 同二进制 A/B 验证，实测为准 |
| 跨 Publisher 提交 bug | 高 | 低 | 运行时校验 + 单元测试 |
| 生命周期悬空 | 高 | 低 | Debug 断言 + 代码审查 |
| p99 延迟恶化超预期 | 中 | 中 | 预注册 p99 门槛，超标则回滚 |

---

## 6. 时间线与资源

| Phase | 任务 | 工期 | 负责人 |
|-------|------|------|--------|
| 1 | 核心实现与测试 | 3 周 | 核心团队 |
| 2 | 内部迁移与编译修复 | 2 周 | 核心团队 |
| 3 | 性能验证 | 2 周 | QA + 核心团队 |
| 4 | 文档与发布 | 1 周 | 文档 + 发版 |
| **总计** | | **8 周** | |

---

## 7. 成功标准

1. ✅ 所有单元测试通过（含跨 Publisher 提交拒绝）
2. ⏳ 64 B claim API 吞吐：第一次正式 S/B 中位数比 1.258804 判 GO；第二次 1.238393
   判 INCONCLUSIVE，p=0.002165、6/6 组一致。收益方向稳定，但 GO 未复现
3. ⏳ 延迟：正式 attempt/planned 矩阵已完成；短测 p50 接近，但 p99/p99.9 轮间尖峰与
   未重跑 HFT 长尾意味着不能宣告延迟非劣
4. ✅ 内部迁移完成：无 `claim.commit()` 调用点
5. ✅ 迁移文档完整，用户可自助迁移
6. ✅ v4.0.0 已由用户明确批准发布。用户确认两次压测均有效，只是未达到理论收益值；
   GO 未复现且延迟长尾未验收的事实保留在 §4.7–4.8 与发布说明中，不作为通过声明

---

## 8. 批准与授权

**提议人**: Phase 3 E2 实验团队  
**审批人**: 用户  
**批准日期**: 2026-10-22  

**后续动作**:
1. 完成 claim API 与 A/B 工具评审，发布 v4.0.0；压测有效但未达到理论收益值
2. 单独决定是否重跑 HFT 长尾门禁；不得把本轮短延迟结果替代该门禁
3. 发布范围与性能边界已写入 CHANGELOG 和 release note；后续如修复长尾，另立实验

---

## 附录

### A. E2 实验证据

- **实验目录**: `build/benchmarks/p3-e2-claim-20261022-154552/`
- **分析报告**: `build/benchmarks/p3-e2-claim-20261022-154552/analysis.md`
- **判定**: 强候选（99.3% 回收，6/6 组一致）

### B. 延迟数据（E2 实验）

| 配置 | 臂 | p50 | p99 | p99.9 |
|------|---|-----|-----|-------|
| latency-64b-planned | ST | 307 | 3215 | 16383 |
| latency-64b-planned | MT-O | 311 | 3759 | 24063 |
| latency-64b-planned | MT-B | 309 | 4959 | 17535 |

**p99 恶化**: 3759 → 4959 ns (~32%)  
**p99.9 可接受**: 24063 → 17535 ns (改善)

### C. 参考文献

1. Phase 3 E2 实验计划: `doc/plans/phase3-e2-aeron-claim-isolation-plan.md`
2. Phase 3 E2 实验报告: `doc/plans/phase3-e2-claim-report.md`
3. Aeron BufferClaim: [github.com/real-logic/aeron](https://github.com/real-logic/aeron)

### D. 设计受 Aeron 启发但非完全一致

Aeron 的 `BufferClaim` 是可复用对象，调用 `claim.commit()`；salias v4.0.0 改为值语义 claim + `publisher.commit(claim)`，适配 C++ RAII 和 salias 生命周期模型。
