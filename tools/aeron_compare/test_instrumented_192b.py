#!/usr/bin/env python3
"""Regression tests for 192 B high/low runtime-mode counter comparison."""

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
from analyze_instrumented_192b import summarize


class Instrumented192BTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)

    def build(self, cases):
        (self.output / "environment.json").write_text(
            json.dumps({"rounds": len(cases), "messages": 1000})
        )
        records = []
        for round_number, (rate, backpressure, empty, polls) in enumerate(cases, 1):
            stdout = self.output / f"{round_number:03d}.stdout"
            stdout.write_text(f"RESULT publish_msg_per_sec={rate}\n")
            diagnostic = {
                "diagnostic_mode": "counters",
                "diagnostic_claim_success": "1000",
                "diagnostic_claim_backpressure": str(backpressure),
                "diagnostic_claim_yield_cycles": str(backpressure),
            }
            for consumer in range(2):
                prefix = f"diagnostic_consumer{consumer}_"
                diagnostic.update(
                    {
                        prefix + "poll_empty": str(empty),
                        prefix + "poll_success": str(polls),
                        prefix + "poll_total_messages": "1000",
                    }
                )
            records.append(
                {
                    "number": round_number,
                    "round": round_number,
                    "phase": "measured",
                    "messages": 1000,
                    "result": {
                        "scenario": "fifo",
                        "published": "1000",
                        "delivered": "2000",
                        "producers": "1",
                        "consumers": "2",
                        "payload": "192",
                        "publish_msg_per_sec": str(rate),
                    },
                    "diagnostic": diagnostic,
                    "stdout": stdout.name,
                    "stdout_sha256": hashlib.sha256(stdout.read_bytes()).hexdigest(),
                }
            )
        (self.output / "results.jsonl").write_text(
            "".join(json.dumps(record) + "\n" for record in records)
        )
        files = sorted(path for path in self.output.iterdir() if path.is_file())
        (self.output / "SHA256SUMS").write_text(
            "".join(
                f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
                for path in files
            )
        )

    def test_modes_and_intermediate_rounds_are_separated(self):
        self.build(
            [
                (17_000_000, 1, 10, 100),
                (16_000_000, 1, 10, 100),
                (8_000_000, 200, 100, 100),
                (12_000_000, 50, 50, 100),
            ]
        )
        result = summarize(self.output)
        self.assertEqual(result["mode_rounds"], {"high": 2, "low": 1, "intermediate": 1})
        self.assertEqual(result["measured_rounds"], 4)
        self.assertGreater(result["answers"]["backpressure_low_to_high_ratio"], 100)
        self.assertGreater(result["answers"]["consumer_empty_low_to_high_ratio"], 5)
        self.assertEqual(result["answers"]["largest_absolute_relative_change"], "backpressure_rate")

    def test_checksum_mismatch_is_rejected(self):
        self.build([(17_000_000, 1, 10, 100), (8_000_000, 200, 100, 100)])
        (self.output / "001.stdout").write_text("changed\n")
        with self.assertRaisesRegex(ValueError, "checksum"):
            summarize(self.output)

    def test_missing_mode_is_rejected(self):
        self.build([(17_000_000, 1, 10, 100), (12_000_000, 10, 50, 100)])
        with self.assertRaisesRegex(ValueError, "Need both"):
            summarize(self.output)

    def test_single_mode_can_be_reported_without_comparison(self):
        self.build([(6_000_000, 200, 100, 100), (5_000_000, 100, 100, 100)])
        result = summarize(self.output, require_both_modes=False)
        self.assertEqual(
            result["mode_rounds"], {"high": 0, "low": 2, "intermediate": 0}
        )
        self.assertIsNone(result["comparison"])
        self.assertIsNone(result["answers"]["backpressure_low_to_high_ratio"])
        self.assertIsNone(result["answers"]["consumer_empty_low_to_high_ratio"])
        self.assertIsNone(result["answers"]["largest_absolute_relative_change"])
        self.assertEqual(result["low"]["throughput"]["rounds"], 2)


if __name__ == "__main__":
    unittest.main()
