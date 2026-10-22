#!/usr/bin/env python3
"""Tests for splitting alternating E1 latency runs by client identity."""

import json
from pathlib import Path
import sys
import tempfile
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))
from compare_latency_ab import LatencyABError, load, summarize  # noqa: E402


BASELINE = "a" * 64
CANDIDATE = "b" * 64


def row(round_number, arm, payload=64, origin="attempt"):
    return {
        "origin": origin, "engine": "fifo", "payload": payload, "rate": 1_000_000,
        "duration_s": 2, "producers": 1, "phase": "measured", "round": round_number,
        "arm": arm, "client_sha256": BASELINE if arm == "B" else CANDIDATE,
        "result": {
            "published": "2000000", "delivered": "2000000", "latency_samples": "125001",
            "latency_origin": origin, "publish_msg_per_sec": "1000000",
            "latency_p50_ns": str(250 + round_number + (100 if arm == "C" else 0)),
            "latency_p99_ns": "400", "latency_p999_ns": "4000",
        },
        "resources": {"memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n"},
    }


class LatencyABTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "results.jsonl"

    def write(self, rows):
        self.path.write_text("".join(json.dumps(item) + "\n" for item in rows))

    def test_splits_alternating_rows_by_arm(self):
        rows = [row(number, arm) for number in range(1, 11) for arm in ("B", "C")]
        self.write(rows)
        _, loaded = load(self.path, {"B": BASELINE, "C": CANDIDATE})
        groups = summarize(loaded, BASELINE, CANDIDATE)
        self.assertEqual([(g["arm"], g["round_count"]) for g in groups],
                         [("B", 10), ("C", 10)])

    def test_hash_mismatch_is_rejected(self):
        bad = row(1, "B")
        bad["client_sha256"] = CANDIDATE
        self.write([bad])
        with self.assertRaisesRegex(LatencyABError, "hash"):
            load(self.path, {"B": BASELINE, "C": CANDIDATE})

    def test_duplicate_round_is_rejected(self):
        rows = [row(number, arm) for number in range(1, 11) for arm in ("B", "C")]
        duplicate = row(3, "B")
        rows.append(duplicate)
        self.write(rows)
        _, loaded = load(self.path, {"B": BASELINE, "C": CANDIDATE})
        with self.assertRaisesRegex(LatencyABError, r"measured rounds 1\.\.10"):
            summarize(loaded, BASELINE, CANDIDATE)

    def test_round_count_is_explicit(self):
        rows = [row(number, arm) for number in range(1, 4) for arm in ("B", "C")]
        self.write(rows)
        _, loaded = load(self.path, {"B": BASELINE, "C": CANDIDATE})
        groups = summarize(loaded, BASELINE, CANDIDATE, rounds=3)
        self.assertEqual([(g["arm"], g["round_count"]) for g in groups],
                         [("B", 3), ("C", 3)])
        with self.assertRaisesRegex(LatencyABError, r"measured rounds 1\.\.10"):
            summarize(loaded, BASELINE, CANDIDATE)


if __name__ == "__main__":
    unittest.main()
