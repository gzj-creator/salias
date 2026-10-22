#!/usr/bin/env python3
"""Regression tests for high-mode relative comparison."""

import json
import io
from contextlib import redirect_stdout
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))

from compare_high_mode_rates import (
    compare,
    fisher_exact_greater,
    load_experiment,
    print_markdown,
    required_sample_size,
    two_proportion_test,
)


def rates_for(high_count, total, threshold=15.0):
    """Build deterministic high/low observations without touching disk."""
    return [threshold] * high_count + [threshold - 1.0] * (total - high_count)


def experiment(high_count, total, threshold=15.0):
    """Build the minimal loaded experiment shape used by compare()."""
    high_rates = [threshold] * high_count
    low_rates = [threshold - 1.0] * (total - high_count)
    return {
        "directory": f"/tmp/{high_count}-{total}",
        "environment_sha256": "environment",
        "summary_sha256": "summary",
        "client_sha256": "client",
        "expected_groups": 2,
        "complete": True,
        "groups": [
            {"high_rate": high_count / total},
            {"high_rate": high_count / total},
        ],
        "rounds": total,
        "high_count": high_count,
        "high_rate": high_count / total,
        "high_median": high_rates[0] if high_rates else None,
        "low_median": low_rates[0] if low_rates else None,
        "all_median": threshold,
        "threshold": threshold,
        "messages_per_round": 100,
        "payload_bytes": 192,
        "capacity_bytes": 1048576,
    }


class CompareHighModeRatesTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def build_experiment(self, name, high_groups, rounds=20, threshold=15.0):
        directory = self.root / name
        directory.mkdir()
        groups = []
        for group_idx, high_count in enumerate(high_groups):
            rates = rates_for(high_count, rounds, threshold)
            groups.append({
                "group": group_idx,
                "throughputs": rates,
                "high_mode_count": high_count,
                "median": 16.0 if high_count else 10.0,
                "rounds": rounds,
            })
        (directory / "environment.json").write_text(json.dumps({
            "groups": len(groups),
            "rounds_per_group": rounds,
            "high_mode_threshold_mmsg_s": threshold,
            "client_sha256": "same-client",
            "messages_per_round": 100,
            "payload_bytes": 192,
            "capacity_bytes": 1048576,
        }))
        (directory / "summary.json").write_text(json.dumps(groups))
        return directory

    def test_pooled_two_proportion_test_uses_both_samples(self):
        baseline = {"rounds": 100, "high_count": 72}
        candidate = {"rounds": 100, "high_count": 82}
        result = two_proportion_test(baseline, candidate)
        self.assertAlmostEqual(result["absolute_increase"], 0.10)
        self.assertAlmostEqual(result["p_value_two_sided"], 0.092907, places=6)
        self.assertAlmostEqual(
            fisher_exact_greater(baseline, candidate), 0.06497, places=5
        )

    def test_sample_size_shows_100_rounds_is_underpowered(self):
        self.assertEqual(required_sample_size(0.72, 0.82, 0.05, 0.80), 277)
        self.assertEqual(
            required_sample_size(0.72, 0.82, 0.05, 0.80, two_sided=False), 218
        )

    def test_ten_point_gain_passes_with_large_complete_experiment(self):
        baseline = experiment(288, 400)
        candidate = experiment(328, 400)
        for row in baseline["groups"]:
            row["high_rate"] = 72 / 100
        for row in candidate["groups"]:
            row["high_rate"] = 82 / 100
        result = compare(baseline, candidate, 10.0, 0.05, 15.0)
        self.assertTrue(result["acceptance"]["passed"])
        self.assertTrue(result["acceptance"]["z_test_significant"])
        self.assertTrue(result["acceptance"]["candidate_high_median_ok"])

    def test_unobserved_improvement_cannot_pass(self):
        baseline = experiment(72, 100)
        candidate = experiment(82, 100)
        result = compare(baseline, candidate, 10.0, 0.05, 15.0)
        self.assertFalse(result["acceptance"]["passed"])
        self.assertTrue(result["acceptance"]["increase_threshold_met"])
        self.assertFalse(result["acceptance"]["z_test_significant"])

    def test_regression_cannot_pass_even_if_other_flags_look_valid(self):
        baseline = experiment(328, 400)
        candidate = experiment(288, 400)
        result = compare(baseline, candidate, -10.0, 0.05, 15.0)
        self.assertFalse(result["acceptance"]["passed"])
        self.assertFalse(result["acceptance"]["direction_positive"])
        self.assertTrue(result["acceptance"]["increase_threshold_met"])

    def test_high_median_regression_cannot_pass(self):
        baseline = experiment(288, 400)
        candidate = experiment(360, 400)
        candidate["high_median"] = 14.5
        result = compare(baseline, candidate, 10.0, 0.05, 15.0)
        self.assertFalse(result["acceptance"]["passed"])
        self.assertFalse(result["acceptance"]["candidate_high_median_ok"])

    def test_threshold_mismatch_is_rejected(self):
        baseline = experiment(72, 100)
        candidate = experiment(82, 100, threshold=14.0)
        with self.assertRaisesRegex(ValueError, "thresholds differ"):
            compare(baseline, candidate, 10.0, 0.05, 15.0)

    def test_disk_loader_rejects_incomplete_experiment(self):
        directory = self.build_experiment("partial", [18, 17])
        environment = json.loads((directory / "environment.json").read_text())
        environment["groups"] = 3
        (directory / "environment.json").write_text(json.dumps(environment))
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            load_experiment(directory)
        result = load_experiment(directory, allow_incomplete=True)
        self.assertFalse(result["complete"])

    def test_disk_loader_rejects_inconsistent_stored_count(self):
        directory = self.build_experiment("bad-count", [18, 17])
        groups = json.loads((directory / "summary.json").read_text())
        groups[0]["high_mode_count"] = 19
        (directory / "summary.json").write_text(json.dumps(groups))
        with self.assertRaisesRegex(ValueError, "Stored high-mode count"):
            load_experiment(directory)

    def test_disk_loader_uses_recorded_threshold(self):
        directory = self.build_experiment("threshold", [18, 17], threshold=14.0)
        result = load_experiment(directory)
        self.assertEqual(result["high_count"], 35)
        self.assertEqual(result["threshold"], 14.0)

    def test_condition_mismatch_is_rejected(self):
        baseline = experiment(72, 100)
        candidate = experiment(82, 100)
        candidate["capacity_bytes"] = 4 * 1024 * 1024
        with self.assertRaisesRegex(ValueError, "conditions differ"):
            compare(baseline, candidate, 10.0, 0.05, 15.0)

    def test_missing_condition_metadata_is_rejected(self):
        baseline = experiment(72, 100)
        candidate = experiment(82, 100)
        baseline["payload_bytes"] = None
        with self.assertRaisesRegex(ValueError, "conditions differ"):
            compare(baseline, candidate, 10.0, 0.05, 15.0)

    def test_missing_condition_can_only_be_diagnostic(self):
        baseline = experiment(288, 400)
        candidate = experiment(328, 400)
        baseline["payload_bytes"] = None
        result = compare(
            baseline,
            candidate,
            10.0,
            0.05,
            15.0,
            allow_missing_conditions=True,
        )
        self.assertFalse(result["acceptance"]["passed"])
        self.assertFalse(result["acceptance"]["evidence_complete"])
        self.assertEqual(result["acceptance"]["missing_conditions"], ["payload_bytes"])

    def test_markdown_output_contains_rates_and_acceptance(self):
        result = compare(experiment(288, 400), experiment(328, 400), 10.0, 0.05, 15.0)
        output = io.StringIO()
        with redirect_stdout(output):
            print_markdown(result)
        text = output.getvalue()
        self.assertIn("| High mode | 288/400 (72.0%) | 328/400 (82.0%) |", text)
        self.assertIn("Acceptance: PASS", text)


if __name__ == "__main__":
    unittest.main()
