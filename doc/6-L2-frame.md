# 6 - L2 帧格式层

> 上层文档：`3-layered-architecture-overview.md`
> 一句话职责：把 payload 编码为「8 字节帧头 + 对齐 payload」的字节序列，并纯函数式地解码回帧头——只管编解码与对齐，不管数据从哪来往哪去。

**技术栈**：C++20。namespace `salias::frame`，目录 `core/frame/`。依赖 L1（连续切片），绝不依赖 L3+。**编解码是纯函数、不持状态**（层间契约 L2↔L3）。

---

## 1. 职责与边界

**做：** 定义帧头二进制布局；提供 `encode_header` / `decode_header`（纯函数）；定义对齐与帧长计算；提供无头定长模式。

**不做：** 不读写 ring（那是 L3 借 L1 做的）、不管 position/背压、不持有任何状态。

---

## 2. 与 Aeron 32 字节头的对比

Aeron `DataHeaderFlyweight` = 32 字节：

| Aeron 字段 | 字节 | salias 是否保留 | 理由 |
|-----------|------|----------------|------|
| frame length | 4 | **保留**（`len`） | 必需 |
| version | 1 | 去 | 版本由 channel 元数据统一描述，不必逐帧带 |
| flags | 1 | **保留**（并入 `meta`） | 需要 BEGIN/END/COMMIT 等 |
| type | 2 | 去/可选 | 单通道点对点类型固定；需要时用 `meta` 位 |
| term offset | 4 | 去 | magic ring 无 term 概念 |
| session id | 4 | 去 | 单通道点对点冗余，channel 已知 |
| stream id | 4 | 去 | 同上 |
| term id | 4 | 去 | 无 term |
| （对齐/保留） | 8 | 去 | — |

结论：点对点场景下，除 `len` 与少量 flags/seq 外，其余字段要么恒定、要么可由 channel 元数据描述。salias 砍到 **8 字节**，对齐从 32 降到 **8**。

---

## 3. 8 字节帧头布局

```
 byte:  0   1   2   3   4   5   6   7
       +---+---+---+---+---+---+---+---+
       |   len (u32, LE)   |  meta (u32, LE)  |
       +---+---+---+---+---+---+---+---+
        └ payload 字节数 ┘  └ flags + seq ┘
```

```cpp
namespace salias::frame {

inline constexpr std::size_t kHeaderSize = 8;
inline constexpr std::size_t kFrameAlign = 8;   // 对比 Aeron 32

// meta 低 8 位为 flags，高 24 位为轮次序号（用于消费者检测撕裂/回绕代次）。
enum Flags : std::uint32_t {
  FLAG_BEGIN     = 1u << 0,   // 多帧消息起始（bulk 用；标准帧 BEGIN|END 同置）
  FLAG_END       = 1u << 1,   // 多帧消息结束
  FLAG_PADDING   = 1u << 2,   // 空洞/对齐填充帧，消费者跳过
  FLAG_COMMITTED = 1u << 3,   // 已提交可见（配合 L3 position release）
};

struct FrameHeader {
  std::uint32_t len;    // payload 长度（不含头、不含对齐填充）
  std::uint32_t meta;   // 低 8 位 flags | 高 24 位 seq
};
static_assert(sizeof(FrameHeader) == kHeaderSize);

} // namespace salias::frame
```

- 固定小端（LE）——目标仅 Linux/x86_64、ARM64 均 LE，不做字节序转换（同机 IPC 无异构问题）。
- `meta` 里的 `seq`（24 位轮次）帮助消费者在无锁读时识别"这一格是否是本代次的新数据"（结合 position，主判据仍是 L3 的 position）。

---

## 4. 对齐与帧长

```cpp
inline constexpr std::size_t align_up(std::size_t n) noexcept {
  return (n + (kFrameAlign - 1)) & ~(kFrameAlign - 1);   // 位运算，无除法
}
// 一帧在 ring 中占用的总字节：头 + 对齐后的 payload。
inline constexpr std::size_t frame_len(std::size_t payload_len) noexcept {
  return kHeaderSize + align_up(payload_len);
}
```

对比：发 8 字节 payload
- Aeron：32 头 + 对齐到 32 的 payload = 32 + 32 = **64 字节**，利用率 12.5%。
- salias：8 头 + 对齐到 8 的 payload = 8 + 8 = **16 字节**，利用率 50%。**4 倍改善**。

---

## 5. 三种模式

### (a) 标准帧
8 字节头 + payload + 对齐填充。`BEGIN|END` 同置表示单帧完整消息。绝大多数小/中消息走这里。

### (b) 无头定长模式（极致小消息吞吐）
channel 元数据统一声明"每条消息固定 N 字节"，帧里**零头**。ring 变成纯 N 字节槽数组，position 步进 N。适合行情 tick、固定结构体等场景——彻底消灭每帧 8 字节头开销与解码分支。

```cpp
// 定长模式下不编解码头，L3 直接按 N 步进；此处仅提供槽计算。
inline constexpr std::size_t fixed_slot(std::size_t record_size) noexcept {
  return align_up(record_size);   // 仍按 8 对齐以避免跨行撕裂
}
```

### (c) padding 帧
magic ring 理论上不需要"跨边界 padding"（双映射保证连续）。但以下情况仍需 padding 帧：
- **对齐空洞**：某些定长/批量场景要让下一帧起点对齐到缓存行，中间留 padding。
- **代次标记**：标记一段被跳过的区间。

诚实说明：本设计的 padding 远少于 Aeron（Aeron 每次 term 边界必 padding），仅在上述可选优化时出现，非必需路径。

---

## 6. 编解码接口（纯函数）

```cpp
namespace salias::frame {

// 编码头到 8 字节缓冲（就地写，调用方给 ring 上的 slice_mut 前 8 字节）。
inline void encode_header(std::span<std::byte, kHeaderSize> dst,
                          std::uint32_t len, std::uint32_t meta) noexcept {
  std::memcpy(dst.data(),     &len,  4);
  std::memcpy(dst.data() + 4, &meta, 4);
}

// 从 8 字节解码（纯函数，无副作用）。
inline FrameHeader decode_header(std::span<const std::byte, kHeaderSize> src) noexcept {
  FrameHeader h;
  std::memcpy(&h.len,  src.data(),     4);
  std::memcpy(&h.meta, src.data() + 4, 4);
  return h;
}

inline std::uint32_t flags(const FrameHeader& h) noexcept { return h.meta & 0xFFu; }
inline std::uint32_t seq(const FrameHeader& h)   noexcept { return h.meta >> 8; }

} // namespace salias::frame
```

**发布原子性注意**：L3 的协议是"先写 payload + 头，再 `store_release(tail)`"。消费者靠 acquire 读 tail 后才读头/ payload，因此头本身不需要单独的原子写——头的可见性由 tail 的 release/acquire 保障。若走"原地可见"的 `COMMITTED` 标志方案，则头的 `meta` 需用 `atomic_ref` 单字 release 写（二选一，由 L3 定策略）。

---

## 7. 边界与校验

```cpp
enum class FrameError { Ok = 0, LenTooLarge, BadFlags, Truncated };
```
- `len` 上限 = ring 容量 - 头 - 对齐余量；超限 -> `LenTooLarge`（交 L5 bulk）。
- 解码时校验：`len` 不超过可读区间（防越界读）、`flags` 无未知高危组合、`seq` 与期望代次一致（否则视为尚未写入的旧槽）。
- 所有校验显式返回 `FrameError`，不抛异常、不越界。

---

## 8. 测试清单

| 测试 | 方法 | 通过标准 |
|------|------|----------|
| 编解码往返 | 随机 (len, flags, seq)（属性/快速检查风格） | `decode(encode(x)) == x` |
| 对齐 | 遍历 payload_len，验证 `frame_len` | 8 对齐、无差一 |
| 极值 | payload=0、=1、=cap-头 | 正确编码，边界不越界 |
| 非法解码 | libFuzzer 喂随机字节 | 不 crash、不越界（ASan/UBSan 干净）、返回 `FrameError` |
| 利用率 | 8 字节 payload 的占用 | =16 字节（相对 Aeron 64 的量化改善） |

---

## 9. 验收标准

- `sizeof(FrameHeader)==8`、`kFrameAlign==8`（`static_assert` 保证）。
- 编解码往返对所有合法输入无损（属性测试通过）。
- libFuzzer 长跑无 crash/UB。
- 8 字节 payload 有效载荷利用率 >= 50%（对比 Aeron 12.5%，量化写入性能报告）。
- 定长无头模式下每消息零头开销（反汇编确认无头编解码分支）。
- 编解码全为纯函数（无静态状态、无 I/O），可并发无锁调用。
