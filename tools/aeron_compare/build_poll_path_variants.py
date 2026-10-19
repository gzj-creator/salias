#!/usr/bin/env python3
"""隔离构建轮询路径实验；保留注册、发布水位和消息借用契约。"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

from build_design_variants import replace_once

ROOT = Path(__file__).resolve().parents[2]
VARIANTS = ("control", "deferred-progress", "branch-scan", "combined",
            "cursor-pack", "cached-registration", "layout-cached")


def pack_cursors(text):
    """把 Rx 五组本地状态聚合为逐环结构，不改共享段布局或游标发布。"""
    names = ("read_positions_", "next_sequences_", "read_message_sequences_",
             "cached_visible_pos_", "flushed_positions_")
    fields = ("position", "sequence", "messages", "visible", "flushed")
    initializers = "\n".join(f"          {name}(channel.producer_rings_.size(), 0)," for name in names)
    text = replace_once(text, initializers, "          cursors_(channel.producer_rings_.size()),")
    text = replace_once(text, "      flushed_positions_ = read_positions_;",
        "      for (auto& cursor : cursors_) { cursor.flushed = cursor.position; }")
    declarations = re.compile(r"    std::vector<std::uint64_t> (?:" + "|".join(names) + r");[^\n]*\n")
    if len(declarations.findall(text)) != 5:
        raise ValueError("cursor declaration anchors changed")
    text = declarations.sub("", text)
    text = replace_once(text, "    std::vector<bool> active_leases_;", """    struct RingCursor {
      std::uint64_t position = 0;
      std::uint64_t sequence = 0;
      std::uint64_t messages = 0;
      std::uint64_t visible = 0;
      std::uint64_t flushed = 0;
    };
    std::vector<RingCursor> cursors_;
    std::vector<bool> active_leases_;""")
    for name, field in zip(names, fields):
        text = re.sub(re.escape(name) + r"\[([^\]]+)\]", r"cursors_[\1]." + field, text)
        text = text.replace(name + ".size()", "cursors_.size()")
    return text


def cache_registration(text):
    """缓存固定表形状和指针；每次仍读取 consumer_count 并 acquire 核验代际。"""
    text = replace_once(text,
        "      if (!channel.has_consumer(consumer_id, registration_token)) {", """      registration_shape_valid_ = !channel.producer_rings_.empty() &&
          consumer_id < channel.producer_rings_.front().shared.consumers.size();
      if (registration_shape_valid_) {
        registration_state_ =
            channel.producer_rings_.front().shared.consumers[consumer_id].registration_state;
      }
      if (!channel.has_consumer(consumer_id, registration_token)) {""")
    text = replace_once(text, "    bool has_pending_progress() const noexcept {", """    bool has_cached_registration() const noexcept {
      if (channel_ == nullptr || consumer_id_ >= channel_->consumer_count() ||
          !registration_shape_valid_) {
        return false;
      }
      if (registration_state_ == nullptr) {
        return registration_token_ == 0;
      }
      return (registration_token_ & consumer_registration::kPhaseMask) ==
                 consumer_registration::kActive &&
             std::atomic_ref<std::uint64_t>(*registration_state_)
                     .load(std::memory_order_acquire) == registration_token_;
    }

    std::uint64_t* registration_state_ = nullptr;
    bool registration_shape_valid_ = false;

    bool has_pending_progress() const noexcept {""")
    for signature, end_signature in (
            ("    std::uint32_t try_recv_run(flow::Message* out,", "    flow::Message recv() noexcept"),
            ("    void flush_progress() noexcept {", "   private:")):
        start = text.index(signature)
        end = text.index(end_signature, start)
        body = replace_once(text[start:end],
            "!channel_->has_consumer(consumer_id_, registration_token_)",
            "!has_cached_registration()")
        text = text[:start] + body + text[end:]
    return text


def transform(text, variant):
    """只改非丢旧批量接收；实验不修改产品文件。"""
    if variant in ("cursor-pack", "layout-cached"):
        text = pack_cursors(text)
    if variant in ("cached-registration", "layout-cached"):
        text = cache_registration(text)
    start = text.index("    std::uint32_t try_recv_run(flow::Message* out,")
    end = text.index("    flow::Message recv() noexcept", start)
    body = text[start:end]
    if variant in ("deferred-progress", "combined"):
        body = replace_once(body,
            "        const bool no_pending_at_entry = !has_pending_progress();",
            "        bool checked_pending = false;\n        bool no_pending_at_entry = false;")
        body = replace_once(body, "          std::uint32_t produced = 0;", """          // 空轮询不改消费进度，无须扫描 pending 向量；首次可读时再取入口快照。
          if (!checked_pending) {
            no_pending_at_entry = !has_pending_progress();
            checked_pending = true;
          }
          std::uint32_t produced = 0;""")
    if variant in ("branch-scan", "combined"):
        body = replace_once(body,
            """        for (std::size_t scanned = 0; scanned < count; ++scanned) {
          const std::uint32_t producer_id =
              static_cast<std::uint32_t>((last_hit_ring_ + scanned) % count);""",
            """        std::size_t producer_id = last_hit_ring_;
        for (std::size_t scanned = 0; scanned < count;
             ++scanned, producer_id = producer_id + 1 == count ? 0 : producer_id + 1) {""")
        body = replace_once(body,
            "last_hit_ring_ = static_cast<std::uint32_t>((producer_id + 1) % count);",
            "last_hit_ring_ = static_cast<std::uint32_t>(producer_id + 1 == count ? 0 : producer_id + 1);")
    if variant not in ("control", "deferred-progress", "branch-scan", "combined",
                        "cursor-pack", "cached-registration", "layout-cached"):
        raise ValueError(f"unknown variant: {variant}")
    return text[:start] + body + text[end:]


def verify_api(output, variants, release):
    """串行重链已验证的 Release 测试对象，运行每个隔离实现的完整 API 套件。"""
    inputs = [release / "test/CMakeFiles/salias_api_tests.dir/api/channel_api_test.cpp.o",
              release / "test/CMakeFiles/salias_api_tests.dir/api/latest_subscription_test.cpp.o"]
    libraries = [release / "src/salias/CMakeFiles/salias.dir/src/latest_by_key.cpp.o",
                 Path("/usr/lib/x86_64-linux-gnu/libgtest_main.a"),
                 release / "src/core/libsalias_core.a",
                 Path("/usr/lib/x86_64-linux-gnu/libgtest.a")]
    shared = (ROOT / "src/core/channel/shared_hybrid_mpsc.hpp").read_text()
    channel = (ROOT / "src/salias/src/channel.cpp").read_text()
    results = output / "verification"
    results.mkdir(parents=True, exist_ok=True)
    for variant in variants:
        directory = output / variant
        if ((directory / "include/core/channel/shared_hybrid_mpsc.hpp").read_text() !=
                transform(shared, variant) or (directory / "channel.cpp").read_text() != channel):
            raise ValueError(f"source snapshot no longer matches product: {variant}")
        binary = output / "bin" / f"{variant}-api"
        command = ["c++", "-O3", "-DNDEBUG", "-flto=1", "-fno-fat-lto-objects",
                   *map(str, inputs), str(directory / "channel.o"), *map(str, libraries),
                   "-pthread", "-o", str(binary)]
        with (results / f"{variant}.link.stderr").open("w") as err:
            subprocess.run(command, stderr=err, check=True)
        report = results / f"{variant}.api.json"
        run_command = [str(binary), f"--gtest_output=json:{report}"]
        with (results / f"{variant}.api.stdout").open("w") as out, \
                (results / f"{variant}.api.stderr").open("w") as err:
            subprocess.run(run_command, stdout=out, stderr=err, check=True, timeout=90)
        result = json.loads(report.read_text())
        if not result["tests"] or result["failures"] or result.get("errors", 0):
            raise ValueError(f"API verification failed: {variant}")
        record = dict(variant=variant, tests=result["tests"], failures=result["failures"],
                      reused_release_test_objects=True, link_command=command, run_command=run_command,
                      inputs_sha256={str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                                     for path in [*inputs, directory / "channel.o", *libraries]},
                      binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
        (results / f"{variant}.verification.json").write_text(json.dumps(record, indent=2) + "\n")
        print(json.dumps(dict(variant=variant, api_tests=result["tests"], failures=result["failures"])),
              flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--variants", nargs="+", choices=VARIANTS,
                        default=["control", "deferred-progress", "branch-scan", "combined"])
    parser.add_argument("--verify-api-only", action="store_true",
                        help="reuse existing variant objects and current Release API test objects")
    parser.add_argument("--release-build", type=Path, default=ROOT / "build/release")
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    (out / "bin").mkdir(exist_ok=True)
    if args.verify_api_only:
        verify_api(out, args.variants, args.release_build.resolve())
        return
    shared = (ROOT / "src/core/channel/shared_hybrid_mpsc.hpp").read_text()
    benchmark = (ROOT / "tools/aeron_compare/salias_ipc_compare.cpp").read_text()
    channel = (ROOT / "src/salias/src/channel.cpp").read_text()
    for header in ("latency_histogram.hpp", "latency_pacing.hpp"):
        (out / header).write_text((ROOT / "tools/aeron_compare" / header).read_text())
    for variant in args.variants:
        directory = out / variant
        include = directory / "include"
        (include / "core/channel").mkdir(parents=True, exist_ok=True)
        changed = transform(shared, variant)
        (include / "core/channel/shared_hybrid_mpsc.hpp").write_text(changed)
        (directory / "channel.cpp").write_text(channel)
        (directory / "compare.cpp").write_text(benchmark)
        flags = ["c++", "-std=c++23", "-O3", "-g", "-DNDEBUG", "-march=native", "-flto=1",
                 "-I", str(include), "-I", str(ROOT / "src/salias/include"), "-I", str(ROOT / "src")]
        obj = str(directory / "channel.o")
        subprocess.run([*flags, "-c", str(directory / "channel.cpp"), "-o", obj], check=True)
        subprocess.run([*flags, "-I", str(out), str(directory / "compare.cpp"), obj,
                        str(ROOT / "build/release/src/core/libsalias_core.a"), "-pthread", "-lrt",
                        "-o", str(out / "bin" / variant)], check=True)
        subprocess.run([*flags, str(ROOT / "test/api/poll_storage_contract_test.cpp"), obj,
                        str(ROOT / "build/release/src/core/libsalias_core.a"), "-pthread", "-lrt",
                        "-o", str(out / "bin" / f"{variant}-contract")], check=True)
        subprocess.run([str(out / "bin" / f"{variant}-contract")], check=True)
        print(json.dumps(dict(variant=variant, header_sha256=hashlib.sha256(changed.encode()).hexdigest(),
                              binary=str(out / "bin" / variant), contract="passed")), flush=True)


if __name__ == "__main__":
    main()
