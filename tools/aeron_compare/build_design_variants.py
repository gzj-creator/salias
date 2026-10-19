#!/usr/bin/env python3
"""Build experimental source copies; never modify product sources or installation."""

import argparse
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f"variant anchor changed: {old[:80]}")
    return text.replace(old, new, 1)


def lazy_poll(text):
    text = eager_poll(text)
    # Only initialize the large descriptor array after delivering the first message.
    old = """    // 按批处理，每批最多 kChunk 条；用栈上缓冲区暂存，统一 fetch 后逐条回调再 flush。
    constexpr std::uint32_t kChunk = 32;
    Message buffer[kChunk];
    std::size_t consumed = 0;"""
    new = """    // 空轮询和首条回调不初始化整个批缓冲；payload 仍在回调结束后才释放。
    Message first;
    if (fetch_batch(&first, 1) == 0) {
      return 0;
    }
    handler(first);
    flush_batch();
    if (max_messages == 1) {
      return 1;
    }
    constexpr std::uint32_t kChunk = 32;
    Message buffer[kChunk];
    std::size_t consumed = 1;"""
    return replace_once(text, old, new)


def eager_poll(text):
    """Restore the dated eager-array control without changing current product code."""
    current = "    detail::PollMessageBuffer<kChunk> storage;\n    Message* buffer = storage.data();"
    return replace_once(text, current, "    Message buffer[kChunk];") if current in text else text


def split_cursor(text):
    # Use previously unused padding: slot size, consumer offsets and segment size stay fixed.
    text = replace_once(text, "  std::uint64_t local_sequence = 0;  ///< 本地已 claim", "  alignas(64) std::uint64_t local_sequence = 0;  ///< 本地已 claim")
    return replace_once(text, "hot_pad[kCacheLineGuard - 8 * sizeof(std::uint64_t)]",
                        "hot_pad[kCacheLineGuard - 64 - 7 * sizeof(std::uint64_t)]")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--variants", nargs="+", choices=["baseline", "eager", "frame-acquire", "frame-checked", "lazy", "split", "combined", "frame64", "pause", "yield", "watchdog256", "single", "chunk1", "scratch", "ready"], default=["baseline", "lazy", "split", "combined", "frame64"])
    parser.add_argument("--allow-unsafe-frame-prototype", action="store_true")
    args = parser.parse_args()
    if "frame-acquire" in args.variants and not args.allow_unsafe_frame_prototype:
        parser.error("frame-acquire fails stale-payload safety; explicit diagnostic opt-in required")
    out = args.output.resolve()
    (out / "bin").mkdir(parents=True, exist_ok=True)
    # Keep the original poll/watchdog body. Runtime diagnostic dispatch can change
    # LTO inlining enough to create a larger difference than the tested design.
    benchmark = (ROOT / "tools/aeron_compare/salias_ipc_compare.cpp").read_text()
    out.mkdir(parents=True, exist_ok=True)
    (out / "fixed_loop_compare.cpp").write_text(benchmark)
    # Local includes of benchmark support headers resolve beside the generated file.
    for header_name in ("latency_histogram.hpp", "latency_pacing.hpp"):
        (out / header_name).write_text((ROOT / "tools/aeron_compare" / header_name).read_text())
    for variant in args.variants:
        directory = out / variant
        include = directory / "include"
        (include / "salias").mkdir(parents=True, exist_ok=True)
        channel = (ROOT / "src/salias/src/channel.cpp").read_text()
        header = (ROOT / "src/salias/include/salias/subscriber.hpp").read_text()
        source = benchmark
        if variant in ("lazy", "combined", "scratch", "ready", "eager"):
            header = eager_poll(header)
        if variant == "eager":
            channel = replace_once(channel,
                "  detail::PollMessageBuffer<kChunk> storage;\n  Message* buffer = storage.data();",
                "  Message buffer[kChunk];")
        if variant in ("frame-acquire", "frame-checked"):
            from frame_acquire_prototype import transform
            shared = transform((ROOT / "src/core/channel/shared_hybrid_mpsc.hpp").read_text(),
                               checked=variant == "frame-checked")
            (include / "core/channel").mkdir(parents=True, exist_ok=True)
            (include / "core/channel/shared_hybrid_mpsc.hpp").write_text(shared)
        if variant == "single":
            source = replace_once(source, "      static_cast<void>(subscriber.poll(options.poll_limit, handler));",
                "      if (auto message = subscriber.try_recv()) { handler(*message); subscriber.release(*message); }")
        if variant in ("pause", "yield"):
            source = replace_once(source, "      static_cast<void>(subscriber.poll(options.poll_limit, handler));",
                "      if (subscriber.poll(options.poll_limit, handler) == 0) { " +
                ("__builtin_ia32_pause();" if variant == "pause" else "std::this_thread::yield();") + " }")
        if variant == "watchdog256":
            source = replace_once(source, "    while (consumed < expected) {\n      if (has_failed(state)",
                "    unsigned watchdog = 0;\n    while (consumed < expected) {\n      if ((++watchdog & 255) == 0 && (has_failed(state)")
            source = replace_once(source, "std::chrono::steady_clock::now() > deadline) {\n        mark_failed(state);\n        _exit(22);",
                "std::chrono::steady_clock::now() > deadline)) {\n        mark_failed(state);\n        _exit(22);")
        if variant == "chunk1":
            header = replace_once(header, "constexpr std::uint32_t kChunk = 32;", "constexpr std::uint32_t kChunk = 1;")
        if variant == "scratch":
            # Prototype only: endpoint-local scratch, NOT suitable for recursive poll.
            channel = replace_once(channel, "  bool valid = false;  ///< consumer id 是否有效。",
                "  Message scratch[32];\n  bool valid = false;  ///< consumer id 是否有效。")
            header = replace_once(header, "    Message buffer[kChunk];", "    Message* buffer = batch_buffer();\n    if (!buffer) { return 0; }")
            header = replace_once(header, "  void flush_batch() noexcept;", "  void flush_batch() noexcept;\n  Message* batch_buffer() noexcept;")
            channel = replace_once(channel, "/// @brief 提交 fetch_batch 累积的消费进度",
                "template <Mode M>\nMessage* Subscriber<M, RawPayloadSchema>::batch_buffer() noexcept {\n  return endpoint_ && endpoint_->valid ? endpoint_->scratch : nullptr;\n}\n\n/// @brief 提交 fetch_batch 累积的消费进度")
        if variant == "ready":
            header = replace_once(header, "    Message buffer[kChunk];", "    if (!batch_maybe_ready()) { return 0; }\n    Message buffer[kChunk];")
            header = replace_once(header, "  void flush_batch() noexcept;", "  void flush_batch() noexcept;\n  bool batch_maybe_ready() noexcept;")
            channel = replace_once(channel, "/// @brief 提交 fetch_batch 累积的消费进度",
                "template <Mode M>\nbool Subscriber<M, RawPayloadSchema>::batch_maybe_ready() noexcept {\n  return endpoint_ && endpoint_->valid && endpoint_->rx.has_visible_data();\n}\n\n/// @brief 提交 fetch_batch 累积的消费进度")
            shared = (ROOT / "src/core/channel/shared_hybrid_mpsc.hpp").read_text()
            shared = replace_once(shared, "    std::uint32_t try_recv_run(flow::Message* out, std::uint32_t cap) noexcept {",
                "    bool has_visible_data() const noexcept {\n      if (!channel_) { return false; }\n      for (std::size_t p = 0; p < read_positions_.size(); ++p) {\n        auto* visible = channel_->producer_rings_[p].shared.visible_producer_pos;\n        if (visible && read_positions_[p] < std::atomic_ref<std::uint64_t>(*visible).load(std::memory_order_acquire)) { return true; }\n      }\n      return false;\n    }\n\n    std::uint32_t try_recv_run(flow::Message* out, std::uint32_t cap) noexcept {")
            (include / "core/channel").mkdir(parents=True, exist_ok=True)
            (include / "core/channel/shared_hybrid_mpsc.hpp").write_text(shared)
        if variant in ("lazy", "combined"):
            header = lazy_poll(header)
        if variant in ("split", "combined"):
            channel = split_cursor(channel)
        if variant == "frame64":
            codec = (ROOT / "src/core/frame/codec.hpp").read_text()
            codec = replace_once(codec,
                "return kHeaderSize + (has_expiry ? sizeof(std::uint64_t) : 0) + align_up(payload_len);",
                "return (kHeaderSize + (has_expiry ? sizeof(std::uint64_t) : 0) + align_up(payload_len) + 63) & ~std::size_t{63};")
            (include / "core/frame").mkdir(parents=True, exist_ok=True)
            (include / "core/frame/codec.hpp").write_text(codec)
        (include / "salias/subscriber.hpp").write_text(header)
        (directory / "channel.cpp").write_text(channel)
        (directory / "fixed_loop_compare.cpp").write_text(source)
        flags = ["c++", "-std=c++23", "-O3", "-g", "-DNDEBUG", "-march=native", "-flto=1",
                 "-I", str(include), "-I", str(ROOT / "src/salias/include"), "-I", str(ROOT / "src")]
        subprocess.run([*flags, "-c", str(directory / "channel.cpp"), "-o", str(directory / "channel.o")], check=True)
        subprocess.run([*flags, "-I", str(out), str(directory / "fixed_loop_compare.cpp"),
                        str(directory / "channel.o"), str(ROOT / "build/release/src/core/libsalias_core.a"),
                        "-pthread", "-lrt", "-o", str(out / "bin" / variant)], check=True)
        print(json.dumps(dict(variant=variant, binary=str(out / "bin" / variant))), flush=True)


if __name__ == "__main__":
    main()
