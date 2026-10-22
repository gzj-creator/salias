#!/usr/bin/env python3
"""Regression tests for the independent 192-byte replication analysis."""

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
from analyze_verify_192b import summarize


class Verify192BTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)

    def build(self, rates, rounds=None):
        rounds = len(rates) if rounds is None else rounds
        (self.output / "environment.json").write_text(json.dumps({
            "rounds": rounds, "messages": 100,
        }))
        records = []
        for round_number in range(rounds + 1):
            rate = 17_000_000 if round_number == 0 else rates[round_number - 1]
            result = {
                "published": "100", "delivered": "200", "producers": "1",
                "consumers": "2", "payload": "192", "latency_samples": "0",
                "latency_sample_rate": "0", "rate_per_producer": "0",
                "scenario": "fifo", "publish_msg_per_sec": str(rate),
            }
            stdout = self.output / f"{round_number:03d}.stdout"
            stdout.write_text("RESULT " + " ".join(
                f"{key}={value}" for key, value in result.items()) + "\n")
            records.append({
                "number": round_number + 1, "payload": 192, "round": round_number,
                "phase": "warmup" if round_number == 0 else "measured",
                "result": result, "stdout": stdout.name,
                "stdout_sha256": hashlib.sha256(stdout.read_bytes()).hexdigest(),
            })
        (self.output / "results.jsonl").write_text(
            "".join(json.dumps(record) + "\n" for record in records))
        files = sorted(path for path in self.output.iterdir() if path.is_file())
        (self.output / "SHA256SUMS").write_text("".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
            for path in files))

    def test_stable_high_throughput_is_reproduced(self):
        self.build([16_800_000, 17_000_000, 17_200_000, 17_100_000])
        result = summarize(self.output)
        self.assertEqual(result["measured_runs"], 4)
        self.assertEqual(result["warmup_runs"], 1)
        self.assertTrue(result["decision"]["reproduced_16_to_18_mmsg_s"])
        self.assertEqual(result["outliers"], [])

    def test_bimodal_round_is_an_outlier(self):
        self.build([17_000_000, 17_000_000, 7_000_000, 17_000_000])
        result = summarize(self.output)
        self.assertFalse(result["decision"]["reproduced_16_to_18_mmsg_s"])
        self.assertEqual([row["round"] for row in result["outliers"]], [3])

    def test_missing_round_is_rejected(self):
        self.build([17_000_000, 17_000_000])
        lines = (self.output / "results.jsonl").read_text().splitlines()
        (self.output / "results.jsonl").write_text("\n".join(lines[:-1]) + "\n")
        with self.assertRaisesRegex(ValueError, "complete round"):
            summarize(self.output)


if __name__ == "__main__":
    unittest.main()
