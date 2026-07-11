# salias examples

## 最小 FIFO MPSC

`minimal_fifo.cpp` 创建两个 producer。producer 0 先 claim 但暂不 commit，producer 1 随后发布消息。
FIFO 只保证单 producer 内顺序，因此 consumer 可以先收到 producer 1 的消息：

```bash
./build/release/example/salias_minimal_fifo
```

预期输出：

```text
FIFO received ready producers: 2, 1
```

## 最小 Ordered MPSC

`minimal_ordered.cpp` 同样创建两个 producer，并让全局序号更晚的消息先 commit。Ordered consumer
必须等待更早的全局序号发布，最终仍按 `1, 2` 交付：

```bash
./build/release/example/salias_minimal_ordered
```

预期输出：

```text
Ordered received global sequence: 1, 2
```

## 构建

```bash
cmake --preset release
cmake --build --preset release --target salias_minimal_fifo salias_minimal_ordered
```

原有 `mpsc_subscriber.cpp` 和 `mpsc_publisher.cpp` 展示两个独立进程通过具名共享内存连接的完整 FIFO
MPSC 用法；两个 minimal demo 用于最短路径理解 FIFO 与 Ordered 的语义差异。
