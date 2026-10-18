#!/usr/bin/env python3
"""Regression checks for conservative per-subscriber acceptance decisions."""

import json
from pathlib import Path
import tempfile
import unittest

import summarize
from summary_fixture import make_run


def histogram(value, count):
    index = next(i for i in range(3776) if summarize.bucket_upper(i) >= value)
    h = {"samples": count, "max_sampled_ns": value,
         "above_50us": count if value > 50_000 else 0, "buckets": [[index, count]]}
    for key in ["p50_ns", "p99_ns", "p999_ns"]:
        h[key] = summarize.bucket_upper(index)
    for key in ["p99_pass_50us", "p999_pass_50us"]:
        h[key] = value <= 50_000
    return h


class AcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.rows = make_run()
        self.result = next(row for row in self.rows if row["type"] == "result")
        r = self.result
        for p in r["publishers"]:
            count = p["first_attempt_to_commit"]["samples"]
            p["first_attempt_to_commit"] = histogram(1000, count)
            p["planned_to_first_attempt"] = histogram(1000, count)
        samples = sum(p["first_attempt_to_commit"]["samples"] for p in r["publishers"])
        for sub in r["subscribers"]:
            sub["from_first_attempt"] = histogram(1000, samples)
            sub["from_planned_arrival"] = histogram(1000, samples)
        self.aggregate()

    def aggregate(self):
        for key, subkey in [("aggregate_from_first_attempt", "from_first_attempt"),
                            ("aggregate_from_planned_arrival", "from_planned_arrival")]:
            self.result[key] = summarize.merge([sub[subkey] for sub in self.result["subscribers"]])

    def assess(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in self.rows))
            return summarize.load_phase(path)["acceptance"]

    def test_healthy_complete_fast_input_passes(self):
        self.assertTrue(self.assess()["p99_acceptance_pass"])
        self.assertTrue(self.assess()["p999_acceptance_pass"])

    def test_slow_subscriber_fails_even_if_merged_p99_passes(self):
        sub = self.result["subscribers"][1]
        n = sub["from_planned_arrival"]["samples"]
        fast, slow = histogram(1000, n - n * 15 // 1000), histogram(100_000, n * 15 // 1000)
        sub["from_planned_arrival"] = summarize.merge([fast, slow])
        self.aggregate()
        self.assertTrue(self.result["aggregate_from_planned_arrival"]["p99_pass_50us"])
        assessment = self.assess()
        self.assertFalse(assessment["p99_acceptance_pass"])
        self.assertEqual(assessment["worst_subscriber_p99"], 1)

    def test_worker_failure_cannot_pass_latency(self):
        self.result["worker_failure"] = True
        self.assertFalse(self.assess()["p99_acceptance_pass"])

    def test_incomplete_input_cannot_pass_latency(self):
        # Even if every sent sample is fast, incomplete scheduled input fails.
        self.result["offered_load_completed"] = False
        self.assertFalse(self.assess()["p99_acceptance_pass"])

    def test_integrity_failure_cannot_pass_latency(self):
        self.result["integrity_pass"] = False
        self.assertFalse(self.assess()["p99_acceptance_pass"])

    def test_corrupt_histogram_is_rejected(self):
        self.result["aggregate_from_first_attempt"]["samples"] += 1
        with self.assertRaises(AssertionError):
            self.assess()


if __name__ == "__main__":
    unittest.main()
