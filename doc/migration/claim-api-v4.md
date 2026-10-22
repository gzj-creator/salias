# Claim API 迁移指南：v3.0.0 -> v4.0.0

v4.0.0 移除了 `PublishClaim::commit()`，改为由 `Publisher::commit(std::move(claim))`
提交。`PublishClaim` 不再持有发布端的 `shared_ptr`，因此每条消息不再产生引用计数原子操作。

## 迁移示例

旧代码：

```cpp
auto claim = publisher.try_claim(payload_len);
if (claim) {
  std::memcpy(claim->payload().data(), data, payload_len);
  claim->commit();
}
```

新代码：

```cpp
auto claim = publisher.try_claim(payload_len);
if (claim) {
  std::memcpy(claim->payload().data(), data, payload_len);
  publisher.commit(std::move(*claim));
}
```

schema-bound 通道的调用方式相同：

```cpp
auto claim = publisher.try_claim();
if (claim) {
  std::memcpy(claim->payload().data(), &value, sizeof(value));
  publisher.commit(std::move(*claim));
}
```

## 生命周期契约

- claim 必须由申请它的 Publisher 提交；把 A 的 claim 交给 B 会在调试构建断言，发布构建拒绝，消息不会发布。
- claim 不保活 Publisher。`Publisher` 必须覆盖从 `try_claim()` 到 `commit()` 的完整生命周期。
- 每个成功 claim 都必须提交。未提交 claim 在调试构建析构时断言；Release 构建不检查。
- 同一 Publisher 的副本共享发布端身份，可由任一副本提交。

## 升级检查

```bash
rg -n 'claim(\.|->)commit\(' --glob '!doc/**' --glob '!build/**'
rg -n 'PublishClaim' --glob '!doc/**' --glob '!build/**'
```

仓库内示例、测试和基准已全部迁移；下游需重编译，因为这是破坏性 API/ABI 变更。
