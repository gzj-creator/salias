#!/usr/bin/env python3
"""验证轮询实验的改写范围和协议护栏。"""

import copy
from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from build_poll_path_variants import transform
from export_poll_path_evidence import export

ROOT = Path(__file__).resolve().parents[2]


class PollPathTests(unittest.TestCase):
    def test_control_is_byte_identical(self):
        original = (ROOT / "src/core/channel/shared_hybrid_mpsc.hpp").read_text()
        self.assertEqual(original, transform(original, "control"))

    def test_only_batch_poll_changes_and_guards_survive(self):
        original = (ROOT / "src/core/channel/shared_hybrid_mpsc.hpp").read_text()
        start = original.index("    std::uint32_t try_recv_run(flow::Message* out,")
        end = original.index("    flow::Message recv() noexcept", start)
        for variant in ("deferred-progress", "branch-scan", "combined"):
            with self.subTest(variant=variant):
                changed = transform(original, variant)
                self.assertEqual(changed[:start], original[:start])
                self.assertTrue(changed.endswith(original[end:]))
                self.assertEqual(changed.count("memory_order_acquire"), original.count("memory_order_acquire"))
                self.assertEqual(changed.count("has_consumer(consumer_id_, registration_token_)"),
                                 original.count("has_consumer(consumer_id_, registration_token_)"))

    def test_pending_snapshot_is_after_visible_check_before_any_advance(self):
        text = transform((ROOT / "src/core/channel/shared_hybrid_mpsc.hpp").read_text(), "deferred-progress")
        text = text[text.index("    std::uint32_t try_recv_run(flow::Message* out,"):]
        self.assertLess(text.index("memory_order_acquire"), text.index("!has_pending_progress()"))
        self.assertLess(text.index("!has_pending_progress()"), text.index("channel_->try_read_from_ring"))

    def test_branch_scan_matches_round_robin(self):
        for count in range(1, 65):
            for start in range(count):
                cursor = start
                actual = []
                for _ in range(count):
                    actual.append(cursor)
                    cursor = 0 if cursor + 1 == count else cursor + 1
                self.assertEqual(actual, [(start + offset) % count for offset in range(count)])

    def test_layout_keeps_shared_definitions_and_atomic_orders(self):
        original = (ROOT / "src/core/channel/shared_hybrid_mpsc.hpp").read_text()
        start = original.index("  class Rx {")
        for variant in ("cursor-pack", "layout-cached", "cached-registration"):
            changed = transform(original, variant)
            self.assertEqual(original[:start], changed[:start])
            self.assertEqual(changed.count("memory_order_seq_cst"), original.count("memory_order_seq_cst"))
            self.assertIn("!channel_->has_consumer(consumer_id_, registration_token_)", changed)
            if variant != "cursor-pack":
                self.assertIn("consumer_id_ >= channel_->consumer_count()", changed)
                self.assertIn(".load(std::memory_order_acquire) == registration_token_", changed)
            if variant != "cached-registration":
                self.assertIn("cursors_[producer_id].visible", changed)
                self.assertNotIn("read_positions_[", changed)
                self.assertNotIn("flushed_positions_[", changed)


class PollEvidenceTests(unittest.TestCase):
    def fixture(self, root):
        source = root / "input"
        dataset = source / "screen"
        dataset.mkdir(parents=True)
        raw = dataset / "one.stdout"
        binary = source / "test-binary"
        binary.write_bytes(b"fixture binary")
        row = dict(variant="baseline", origin="attempt", rate=1000000, payload=64,
                   duration_s=2, phase="measured", profiled=False,
                   result=dict(published="2000000", delivered="2000000", latency_samples="125000",
                               **{f"latency_{metric}_ns": "100" for metric in ("p50", "p99", "p999", "max")}),
                   stdout=str(raw), command=[str(binary)],
                   binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                   min_mem_available_kib=1500 * 1024,
                   resources={"memory.events": "high 0\nmax 0\noom 0\noom_kill 0"})
        raw.write_text("RESULT " + " ".join(f"{key}={value}" for key, value in row["result"].items()) + "\n")
        row["stdout_sha256"] = hashlib.sha256(raw.read_bytes()).hexdigest()
        return source, dataset / "results.jsonl", row

    def test_profiles_and_warmup_are_not_performance_population(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            root = Path(directory)
            source, path, row = self.fixture(root)
            rows = [row, {**row, "phase": "warmup"}, {**row, "profiled": True}]
            path.write_text("".join(json.dumps(item) + "\n" for item in rows))
            manifest = export(source, root / "output")
            self.assertEqual(manifest["unprofiled_measured_runs"], 1)
            self.assertEqual(manifest["unprofiled_measured_messages"], 2000000)
            self.assertEqual(manifest["unprofiled_measured_samples"], 125000)
            self.assertEqual(manifest["groups"]["screen"][0]["rounds"], 1)

    def test_reject_tamper_count_and_pressure(self):
        for mutation in ("hash", "count", "pressure", "quantile", "binary"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, path, row = self.fixture(root)
                row = copy.deepcopy(row)
                if mutation == "hash":
                    Path(row["stdout"]).write_text("tampered\n")
                elif mutation == "count":
                    row["result"]["delivered"] = "1"
                elif mutation == "pressure":
                    row["resources"]["memory.events"] = "high 1"
                elif mutation == "quantile":
                    row["result"]["latency_p50_ns"] = "1"
                else:
                    Path(row["command"][0]).write_bytes(b"changed binary")
                path.write_text(json.dumps(row) + "\n")
                with self.assertRaises(ValueError):
                    export(source, root / "output")


if __name__ == "__main__":
    unittest.main()
