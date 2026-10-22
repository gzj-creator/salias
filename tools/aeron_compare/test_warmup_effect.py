#!/usr/bin/env python3
"""Regression tests for the A.2 preload-effect analysis."""

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
from analyze_warmup_effect import summarize


class WarmupEffectTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)

    def build(self, direct, preloaded, cascade):
        environment = {
            "measured_rounds": len(direct), "preload_rounds": 1, "messages": 100,
        }
        (self.output / "environment.json").write_text(json.dumps(environment))
        records = []
        sequence = (
            ("group1-direct-64b", [("measured", 64, value) for value in direct]),
            ("group2-192b-preload", [("preload", 192, 17_000_000)] +
             [("measured", 64, value) for value in preloaded]),
            ("group3-192b-128b-preload", [("preload", 192, 17_000_000),
                                           ("preload", 128, 6_500_000)] +
             [("measured", 64, value) for value in cascade]),
        )
        number = 0
        for group, cases in sequence:
            rounds = {}
            for phase, payload, rate in cases:
                number += 1
                rounds[payload] = rounds.get(payload, 0) + 1
                result = {
                    "published": "100", "delivered": "200", "producers": "1",
                    "consumers": "2", "payload": str(payload), "latency_samples": "0",
                    "latency_sample_rate": "0", "rate_per_producer": "0",
                    "scenario": "fifo", "publish_msg_per_sec": str(rate),
                }
                stdout = self.output / f"{number:03d}.stdout"
                stdout.write_text("RESULT " + " ".join(
                    f"{key}={value}" for key, value in result.items()) + "\n")
                records.append({
                    "number": number, "group": group, "phase": phase, "payload": payload,
                    "round": rounds[payload], "result": result, "stdout": stdout.name,
                    "stdout_sha256": hashlib.sha256(stdout.read_bytes()).hexdigest(),
                    "system_before": {"loadavg": [0, 0, 0], "mem_available_kib": 1000,
                                      "cpu_frequency_hz": {"0": None}},
                })
        (self.output / "results.jsonl").write_text(
            "".join(json.dumps(record) + "\n" for record in records))
        files = sorted(path for path in self.output.iterdir() if path.is_file())
        (self.output / "SHA256SUMS").write_text("".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
            for path in files))

    def test_preload_effect_and_modes_are_reported(self):
        self.build([6_000_000, 7_000_000, 18_000_000],
                   [12_000_000, 12_000_000, 18_000_000],
                   [17_000_000, 17_000_000, 17_000_000])
        result = summarize(self.output)
        direct = result["groups"]["group1-direct-64b"]
        self.assertEqual(direct["rounds"], 3)
        self.assertEqual(direct["high_mode_rounds"], 1)
        self.assertAlmostEqual(result["changes_from_direct_percent"]["group2-192b-preload"],
                               (12_000_000 - 7_000_000) / 7_000_000 * 100)
        self.assertIn("preload changes", result["decision"]["conclusion"])

    def test_no_effect_is_reported(self):
        self.build([6_000_000, 6_000_000], [6_100_000, 6_100_000], [6_000_000, 6_100_000])
        self.assertEqual(summarize(self.output)["decision"]["conclusion"],
                         "no clear preload effect on 64 B")

    def test_missing_preload_round_is_rejected(self):
        self.build([6_000_000], [6_000_000], [6_000_000])
        lines = (self.output / "results.jsonl").read_text().splitlines()
        (self.output / "results.jsonl").write_text("\n".join(lines[:-1]) + "\n")
        with self.assertRaisesRegex(ValueError, "group matrix"):
            summarize(self.output)


if __name__ == "__main__":
    unittest.main()
