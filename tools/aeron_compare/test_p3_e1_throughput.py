#!/usr/bin/env python3
"""Tests for the E1 ABBA orchestration sequence and identity guards."""

import importlib.util
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "p3_e1_throughput", ROOT / "doc/plans/runners/p3-e1-throughput.py"
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class E1ThroughputTest(unittest.TestCase):
    def test_abba_sequence_matches_plan(self):
        self.assertEqual(
            runner.SEQUENCE,
            (("B", 0), ("C", 0), ("C", 1), ("B", 1),
             ("B", 2), ("C", 2), ("C", 3), ("B", 3),
             ("B", 4), ("C", 4), ("C", 5), ("B", 5)),
        )
        for group in range(6):
            self.assertEqual(
                sum(1 for arm, value in runner.SEQUENCE if value == group and arm == "B"), 1
            )
            self.assertEqual(
                sum(1 for arm, value in runner.SEQUENCE if value == group and arm == "C"), 1
            )

    def test_identity_constants_match_frozen_clients(self):
        self.assertEqual(runner.BASELINE_SHA256, "3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da")
        self.assertEqual(runner.CANDIDATE_SHA256, "5efae0b41ed42f9a38d69997ed8a1fafc69d78ae69eea948c79abbdec0f433b6")


if __name__ == "__main__":
    unittest.main()
