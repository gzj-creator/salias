#!/usr/bin/env python3
"""Regression tests for the S.2.2 instrumentation-level comparison."""

import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
from compare_instrumentation_levels import compare


class InstrumentationComparisonTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def add_level(self, level, values):
        directory = self.root / f"instrumentation-L{level}-20261020-120000"
        directory.mkdir()
        records = []
        for round_number, value in enumerate(values, 1):
            records.append(
                {
                    "instrumentation_level": level,
                    "phase": "measured",
                    "result": {"publish_msg_per_sec": str(value * 1_000_000)},
                }
            )
        (directory / "results.jsonl").write_text(
            "".join(json.dumps(record) + "\n" for record in records)
        )
        return directory

    def test_reports_high_rounds_median_and_first_change_point(self):
        values = {
            0: [17.0, 16.0, 15.0],
            1: [8.0, 7.0, 6.0],
            2: [7.5, 7.0, 6.5],
            3: [7.0, 6.0, 5.0],
            4: [6.0, 5.0, 4.0],
        }
        report = compare(
            [self.add_level(level, rounds) for level, rounds in values.items()]
        )
        self.assertEqual(
            [group["high_rounds"] for group in report["groups"]],
            [3, 0, 0, 0, 0],
        )
        self.assertAlmostEqual(report["groups"][1]["median_mmsg_s"], 7.0)
        self.assertAlmostEqual(report["groups"][1]["jump_percent"], -56.25)
        self.assertEqual(report["state_crash_level"], 1)

    def test_missing_level_is_rejected(self):
        directories = [
            self.add_level(level, [10.0 + level])
            for level in range(4)
        ]
        with self.assertRaisesRegex(ValueError, "Missing levels: L4"):
            compare(directories)


if __name__ == "__main__":
    unittest.main()
