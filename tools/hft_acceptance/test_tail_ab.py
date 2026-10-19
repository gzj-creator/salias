#!/usr/bin/env python3
"""Regression checks for independent A/B pooling and acceptance failures."""

import copy
import json
import subprocess
import sys
import shutil
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import summarize_tail_ab
from summary_fixture import histogram, make_run


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
        next(r for r in rows if r["type"] == "result")["integrity_pass"] = False
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

    def test_publishers_are_not_pooled(self):
        result = self.rows[-1]
        result["publishers"][1]["planned_to_first_attempt"] = histogram(9000, 1000)
        output = self.aggregate([self.rows, self.rows])
        for publisher in output["publishers"]:
            original = result["publishers"][publisher["publisher"]]
            self.assertEqual(publisher["planned_to_first_attempt"]["p999_ns"],
                             original["planned_to_first_attempt"]["p999_ns"])
            self.assertEqual(publisher["first_attempt_to_commit"]["samples"], 2000)

    def test_diagnostics_cannot_be_formal_acceptance(self):
        self.rows[0]["diagnostics"] = True
        for pub in self.rows[-1]["publishers"]:
            pub.update(successful_claim=histogram(100, 1000),
                       failed_claim=histogram(900, 2), commit_call=histogram(100, 1000))
        with self.assertRaisesRegex(AssertionError, "diagnostic run"):
            self.aggregate([self.rows])

    def test_formal_diagnostic_histograms_must_be_empty(self):
        self.rows[-1]["publishers"][0]["commit_call"] = histogram(100, 1000)
        with self.assertRaisesRegex(AssertionError, "diagnostic samples"):
            self.aggregate([self.rows])

    def test_stall_injection_cannot_be_formal_acceptance(self):
        self.rows[0]["stall_subscriber_ms"] = 200
        with self.assertRaisesRegex(AssertionError, "fault-injection run"):
            self.aggregate([self.rows])

    def test_diagnostic_sample_counts_must_match(self):
        self.rows[0]["diagnostics"] = True
        for pub in self.rows[-1]["publishers"]:
            pub.update(successful_claim=histogram(100, 999),
                       failed_claim=histogram(900, 2), commit_call=histogram(100, 1000))
        with self.assertRaises(AssertionError):
            self.aggregate([self.rows])

    def test_missing_subscriber_cannot_pass(self):
        self.rows[-1]["subscribers"].pop()
        with self.assertRaises(AssertionError):
            self.aggregate([self.rows])

    def test_publisher_sample_counts_must_match(self):
        self.rows[-1]["publishers"][0]["planned_to_first_attempt"] = histogram(1000, 999)
        with self.assertRaises(AssertionError):
            self.aggregate([self.rows])


class RelativeGateTests(unittest.TestCase):
    @staticmethod
    def version(value=1000, valid=True):
        return {"worst_planned_p999_ns": value, "valid_and_complete": valid,
                "measurement_seconds": 60, "planned_p999_budget_pass": False,
                "run_count": 2, "seconds_per_run": 30}

    def groups(self):
        phases = {}
        for s in (1, 2):
            for p in (64, 256):
                for pattern in ("steady", "burst"):
                    name = f"2p{s}s-{p}b-{pattern}"
                    targeted = s == 2 and pattern == "burst"
                    phases[name] = {"baseline": self.version(),
                                    "candidate": self.version(800 if targeted else 1050)}
        group = {"phases": phases, "order": ["baseline", "candidate", "candidate", "baseline"]}
        return [copy.deepcopy(group) for _ in range(3)]

    def test_exact_twenty_percent_boundary(self):
        gate = summarize_tail_ab.relative_gate
        self.assertTrue(gate("2p2s-64b-burst", self.version(), self.version(800))["pass"])
        self.assertFalse(gate("2p2s-64b-burst", self.version(), self.version(801))["pass"])

    def test_exact_five_percent_boundary(self):
        gate = summarize_tail_ab.relative_gate
        self.assertTrue(gate("2p1s-64b-burst", self.version(), self.version(1050))["pass"])
        self.assertFalse(gate("2p1s-64b-burst", self.version(), self.version(1051))["pass"])

    def test_invalid_or_zero_baseline_fails(self):
        gate = summarize_tail_ab.relative_gate
        self.assertFalse(gate("2p2s-64b-burst", self.version(), self.version(500, False))["pass"])
        self.assertFalse(gate("2p2s-64b-burst", self.version(0), self.version(0))["pass"])

    def test_relative_success_does_not_imply_fifty_us(self):
        result = summarize_tail_ab.assess_groups(self.groups())
        self.assertTrue(result["relative_acceptance_pass"])
        self.assertFalse(result["candidate_50us_pass"])

    def test_one_bad_group_is_not_hidden_by_pooling(self):
        groups = self.groups()
        groups[1]["phases"]["2p2s-256b-burst"]["candidate"] = self.version(801)
        self.assertFalse(summarize_tail_ab.assess_groups(groups)["relative_acceptance_pass"])

    def test_incomplete_run_fails_even_with_large_gain(self):
        groups = self.groups()
        groups[2]["phases"]["2p2s-64b-burst"]["candidate"] = self.version(100, False)
        self.assertFalse(summarize_tail_ab.assess_groups(groups)["relative_acceptance_pass"])

    def test_three_complete_groups_and_fixed_duration_are_required(self):
        for mutation in ("group", "phase", "duration", "count"):
            groups = self.groups()
            if mutation == "group":
                groups.pop()
            elif mutation == "phase":
                del groups[0]["phases"]["2p1s-64b-steady"]
            elif mutation == "duration":
                groups[0]["phases"]["2p1s-64b-steady"]["candidate"]["measurement_seconds"] = 30
            else:
                groups[0]["phases"]["2p1s-64b-steady"]["candidate"]["run_count"] = 1
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                summarize_tail_ab.assess_groups(groups)

    def test_suite_rechecks_raw_files_instead_of_trusting_saved_metrics(self):
        group = {"phases": {"2p2s-64b-burst": {}}}
        for version, rounds in (("baseline", (0, 3)), ("candidate", (1, 2))):
            group["phases"]["2p2s-64b-burst"][version] = {
                "runs": [{"path": f"group-1/round-{r}/{version}/2p2s-64b-burst.jsonl"} for r in rounds],
                "configuration": {"payload_bytes": 64}, "worst_planned_p999_ns": 800}
        with mock.patch.object(summarize_tail_ab, "aggregate", return_value={"tampered": True}):
            with self.assertRaisesRegex(AssertionError, "raw audit"):
                summarize_tail_ab.audit_saved_group(group)

    def test_suite_rejects_duplicate_raw_runs_before_aggregation(self):
        path = "group-1/round-0/baseline/2p2s-64b-burst.jsonl"
        version = {"runs": [{"path": path}, {"path": path}]}
        group = {"phases": {"2p2s-64b-burst": {"baseline": version, "candidate": version}}}
        with self.assertRaisesRegex(AssertionError, "duplicated raw"):
            summarize_tail_ab.audit_saved_group(group)

    def test_suite_cannot_reuse_one_group_as_three_independent_groups(self):
        group = {"phases": {"2p2s-64b-burst": {"baseline": {
            "runs": [{"path": "group-1/round-0/baseline/2p2s-64b-burst.jsonl"}]},
            "candidate": {"runs": []}}}}
        with self.assertRaisesRegex(AssertionError, "another group"):
            summarize_tail_ab.audit_saved_group(group, Path("group-2"))


class ComparisonCliTests(unittest.TestCase):
    def make_directory(self, directory):
        for s in (1, 2):
            for payload in (64, 256):
                for pattern in ("steady", "burst"):
                    name = f"2p{s}s-{payload}b-{pattern}"
                    rows = make_run()
                    start, result = rows
                    planned = 1_300_000 if pattern == "burst" else 1_000_000
                    start.update(subscribers=s, payload_bytes=payload, pattern=pattern,
                                 burst_ms_per_second=100 if pattern == "burst" else 0,
                                 planned_published=planned, cpus=list(range(s + 2)))
                    result.update(planned_published=planned, published=planned,
                                  delivered=planned * s, subscribers=result["subscribers"][:s])
                    for publisher in result["publishers"]:
                        publisher["sent"] = planned // 2
                    for sub in result["subscribers"]:
                        sub["received"] = planned
                        for q in sub["sequence"]:
                            q.update(received=planned // 2, unique=planned // 2)
                    for aggregate, key in (("aggregate_from_first_attempt", "from_first_attempt"),
                                           ("aggregate_from_planned_arrival", "from_planned_arrival")):
                        result[aggregate] = summarize_tail_ab.summarize.merge([sub[key] for sub in result["subscribers"]])
                    for r in range(4):
                        version = "baseline" if r in (0, 3) else "candidate"
                        path = directory / f"round-{r}" / version / f"{name}.jsonl"
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_text("".join(json.dumps(row) + "\n" for row in rows))

    def test_cli_writes_comparison_and_rejects_missing_raw_run(self):
        script = Path(summarize_tail_ab.__file__)
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            self.make_directory(directory)
            command = [sys.executable, str(script), str(directory)]
            completed = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            group = json.loads((directory / "ab-summary.json").read_text())
            self.assertEqual(group["measurement_seconds"], 32)
            self.assertTrue(group["all_valid_and_complete"])
            summarize_tail_ab.audit_saved_group(group, directory)
            (directory / "round-3/baseline/2p2s-256b-burst.jsonl").unlink()
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)

    def test_cli_rejects_mismatched_baseline_candidate_affinity(self):
        script = Path(summarize_tail_ab.__file__)
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            self.make_directory(directory)
            for r in (1, 2):
                path = directory / f"round-{r}/candidate/2p1s-64b-steady.jsonl"
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                rows[0]["cpus"] = [1, 2, 3]
                path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            completed = subprocess.run([sys.executable, str(script), str(directory)], capture_output=True, text=True)
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("A/B configuration mismatch", completed.stderr)


class OrchestrationTests(unittest.TestCase):
    def test_failed_workers_are_preserved_and_suite_cannot_report_success(self):
        root = Path(__file__).resolve().parents[2]
        false = shutil.which("false")
        self.assertIsNotNone(false)
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "failed-suite"
            command = ["bash", "tools/hft_acceptance/run_tail_suite.sh", str(output), false, false]
            completed = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=30)
            self.assertEqual(completed.returncode, 1, completed.stderr)
            self.assertIn("execution_exit=1", (output / "suite-state.txt").read_text())
            self.assertEqual(len(list(output.glob("group-*/round-*/*/*.jsonl"))), 96)
            self.assertFalse((output / "relative-acceptance.json").exists())
            self.assertEqual(subprocess.run(command, cwd=root, capture_output=True).returncode, 2)

    def test_matrix_accepts_explicit_binary_and_refuses_overwrite(self):
        root = Path(__file__).resolve().parents[2]
        false = shutil.which("false")
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "failed-matrix"
            command = ["bash", "tools/hft_acceptance/run_matrix.sh", str(output), "1", false]
            completed = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=15)
            self.assertEqual(completed.returncode, 1, completed.stderr)
            state = (output / "run-state.txt").read_text()
            self.assertIn("exit=1", state)
            self.assertEqual(len(list(output.glob("2p*.jsonl"))), 8)
            self.assertEqual(subprocess.run(command, cwd=root, capture_output=True).returncode, 2)
            self.assertEqual((output / "run-state.txt").read_text(), state)


if __name__ == "__main__":
    unittest.main()
