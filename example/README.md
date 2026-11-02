# salias 示例

示例按使用方式分成两个目录，先确定自己要解决的是“把消息可靠地送到对端”，还是“固定拓扑下的并发吞吐”。

| 目录 | 公开入口 | 底层 ring | 适用场景 |
|---|---|---|---|
| [`simple/`](simple/README.md) | `Channel`、`LatestMpmcChannel` | per-producer `MagicRing` | 变长消息、动态订阅、Ordered、Latest、跨进程入门 |
| [`concurrency/`](concurrency/README.md) | `FixedChannel` | `FastSpscRing` / `FastMpscRing` / `FastSpmcRing` / `FastMpmcRing` | 固定 payload、固定参与者、单 producer lane FIFO、最低热路径开销 |

需要跨 producer 全局顺序、TTL、动态增删参与者或变长消息时用 `simple/`。消息类型和参与者数量在编译期固定，
且每个 producer 只要求自己 lane 内 FIFO 时用 `concurrency/`。

文件命名直接对应公开类型：`simple/` 使用完整的 `Mode` 名（如 `fifo_mpsc`、`ordered_mpmc`），
`concurrency/` 使用完整的 `FixedTopology` 名（如 `spsc`、`mpmc`）。因此两边的段数不同，但都没有省略
协议信息。`Latest` 目前只有 `LatestMpmc`，`Ordered` 目前只有 `OrderedMpsc` 和 `OrderedMpmc`；
具体替代关系见两个目录的 README。

## 构建与测试

```bash
cmake --preset release
cmake --build --preset release
ctest --test-dir build/release -R 'salias_example_' --output-on-failure
```

只构建单个示例时使用对应 target，例如：

```bash
cmake --build --preset release --target salias_example_concurrency_spsc salias_example_simple_ordered_mpsc
```

所有示例都启用 ASan/UBSan 配置；跨进程 MPSC 示例由 `example_smoke.sh` 编排两个进程。
