#!/usr/bin/env python3
"""Regression checks for independent A/B pooling and acceptance failures."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

import summarize_tail_ab
from summary_fixture import make_run


class TailAbTests(unittest.TestCase):
    def setUp(self):
        self.rows = make_run()

    def aggregate(self, runs):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for i, rows in enumerate(runs):
                path = Path(directory) / f"run-{i}.jsonl"
                path.write_text("".join(json.dumps(row) + "\n" for row in rows))
                paths.append(path)
            return summarize_tail_ab.aggregate(paths)

    def test_each_subscriber_keeps_its_own_histogram(self):
        output = self.aggregate([self.rows, self.rows])
        result = next(row for row in self.rows if row["type"] == "result")
        self.assertEqual(len(output["subscribers"]), 2)
        for index, sub in enumerate(output["subscribers"]):
            original = result["subscribers"][index]["from_planned_arrival"]
            merged = sub["from_planned_arrival"]
            self.assertEqual(merged["samples"], original["samples"] * 2)
            self.assertEqual(merged["p999_ns"], original["p999_ns"])
        self.assertEqual(output["published"], result["published"] * 2)

    def test_worker_failure_remains_a_failed_gate(self):
        rows = copy.deepcopy(self.rows)
        next(r for r in rows if r["type"] == "result")["worker_failure"] = True
        output = self.aggregate([self.rows, rows])
        self.assertFalse(output["valid_and_complete"])
        self.assertFalse(output["planned_p999_budget_pass"])

    def test_incomplete_input_is_not_hidden(self):
        rows = copy.deepcopy(self.rows)
        next(r for r in rows if r["type"] == "result")["offered_load_completed"] = False
        self.assertFalse(self.aggregate([rows])["valid_and_complete"])

    def test_changed_rate_is_rejected(self):
        rows = copy.deepcopy(self.rows)
        next(r for r in rows if r["type"] == "start")["steady_total_rate"] = 500000
        with self.assertRaisesRegex(AssertionError, "offered rate changed"):
            self.aggregate([rows])

    def test_changed_window_is_rejected(self):
        rows = copy.deepcopy(self.rows)
        next(r for r in rows if r["type"] == "start")["publication_window_bytes"] = 65536
        with self.assertRaisesRegex(AssertionError, "measurement settings changed"):
            self.aggregate([rows])

    def test_different_configurations_are_not_pooled(self):
        rows = copy.deepcopy(self.rows)
        next(r for r in rows if r["type"] == "start")["payload_bytes"] = 64
        with self.assertRaisesRegex(AssertionError, "configuration mismatch"):
            self.aggregate([self.rows, rows])


if __name__ == "__main__":
    unittest.main()
