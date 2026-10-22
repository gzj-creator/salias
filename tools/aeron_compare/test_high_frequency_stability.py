#!/usr/bin/env python3
"""Regression tests for high-frequency runner and analysis logic."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
spec = importlib.util.spec_from_file_location(
    "high_frequency_runner",
    ROOT / "doc/plans/runners/high-frequency-stability-runner.py",
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
from analyze_high_frequency_stability import load_summary, summarize


class HighFrequencyStabilityTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)

    def build(self, schedules):
        summaries = []
        for group_idx, (interval, sleep_seconds, rates) in enumerate(schedules):
            summaries.append({
                "group": group_idx,
                "interval": interval,
                "sleep_duration": sleep_seconds,
                "throughputs": rates,
                "high_mode_count": sum(rate >= runner.HIGH_MODE_THRESHOLD_MMSG_S
                                       for rate in rates),
                "median": sorted(rates)[len(rates) // 2],
                "rounds": len(rates),
            })
        (self.output / "summary.json").write_text(json.dumps(summaries))

    def test_throughput_parser_reads_result_field(self):
        output = (
            "RESULT library=salias-ipc scenario=fifo publish_msg_per_sec=1.75e+07 "
            "delivery_msg_per_sec=3.5e+07\n"
        )
        self.assertEqual(runner.parse_throughput(output), 17.5)

    def test_throughput_parser_rejects_missing_rate(self):
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            runner.parse_throughput("RESULT scenario=fifo\n")

    def test_interval_assignment_matches_four_blocks(self):
        self.assertEqual(runner.interval_for_group(0), ("no_sleep", 0.0))
        self.assertEqual(runner.interval_for_group(5), ("light_1s", 1.0))
        self.assertEqual(runner.interval_for_group(10), ("medium_10s", 10.0))
        self.assertEqual(runner.interval_for_group(15), ("heavy_60s", 60.0))

    def test_interval_can_be_fixed_for_interleaved_comparisons(self):
        self.assertEqual(runner.interval_for_group(17, 0.0), ("fixed_0s", 0.0))

    def test_resume_rejects_changed_client_or_payload(self):
        client = self.output / "client"
        client.write_bytes(b"one")
        client.chmod(0o755)

        class Args:
            groups = 20
            rounds = 20
            messages = 100
            payload = 192
            capacity = 1048576
            interval_seconds = 0.0
            client = None

        args = Args()
        args.client = client
        runner.write_environment(args, self.output)
        runner.verify_environment(args, self.output)
        args.payload = 64
        with self.assertRaisesRegex(ValueError, "environment differs"):
            runner.verify_environment(args, self.output)
        args.payload = 192
        client.write_bytes(b"two")
        with self.assertRaisesRegex(ValueError, "environment differs"):
            runner.verify_environment(args, self.output)
        client.unlink()
        replacement = self.output / "replacement"
        replacement.write_bytes(b"two")
        replacement.chmod(0o755)
        args.client = replacement
        with self.assertRaisesRegex(ValueError, "environment differs"):
            runner.verify_environment(args, self.output)

    def test_command_preserves_p0_capacity_and_sampling_conditions(self):
        class Args:
            client = Path("/tmp/client")
            messages = 100
            timeout = 60.0
            payload = 192
            capacity = 1048576

        _, command, _ = runner.benchmark_command(Args(), 0, 0)
        command = command[3:]
        pairs = dict(zip(command[1::2], command[2::2]))
        self.assertEqual(pairs["--capacity"], "1048576")
        self.assertEqual(pairs["--payload"], "192")
        self.assertEqual(pairs["--rate"], "0")
        self.assertEqual(pairs["--latency-sample-rate"], "0")
        self.assertEqual(pairs["--latency-origin"], "claim")

    def test_coordinator_is_pinned_to_cpu0(self):
        class Args:
            client = Path("/tmp/client")
            messages = 100
            timeout = 60.0
            payload = 192
            capacity = 1048576

        _, command, _ = runner.benchmark_command(Args(), 0, 0)
        self.assertEqual(command[:3], ["taskset", "-c", "0"])

    def test_payload_and_capacity_are_configurable(self):
        class Args:
            client = Path("/tmp/client")
            messages = 100
            timeout = 60.0
            payload = 64
            capacity = 4 * 1024 * 1024

        _, command, _ = runner.benchmark_command(Args(), 0, 0)
        command = command[3:]
        pairs = dict(zip(command[1::2], command[2::2]))
        self.assertEqual(pairs["--payload"], "64")
        self.assertEqual(pairs["--capacity"], str(4 * 1024 * 1024))

    def test_setenv_is_forwarded_to_systemd_before_the_command(self):
        class Args:
            client = Path("/tmp/client")
            messages = 100
            timeout = 60.0
            payload = 64
            capacity = 1048576
            setenv = ["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"]

        _, command, systemd_command = runner.benchmark_command(Args(), 0, 0)
        forwarded = [entry for entry in systemd_command if entry.startswith("--setenv=")]
        self.assertEqual(
            forwarded,
            ["--setenv=SALIAS_RESOURCE_CAPPED=1",
             "--setenv=SALIAS_DISABLE_ASYMMETRIC_FENCE=1"],
        )
        # 所有 --setenv 必须排在 systemd-run 要执行的命令之前。
        self.assertEqual(systemd_command[-len(command):], command)
        self.assertLess(
            max(index for index, entry in enumerate(systemd_command)
                if entry.startswith("--setenv=")),
            systemd_command.index("taskset"),
        )

    def test_setenv_is_absent_when_not_requested(self):
        class Args:
            client = Path("/tmp/client")
            messages = 100
            timeout = 60.0
            payload = 64
            capacity = 1048576

        _, command, systemd_command = runner.benchmark_command(Args(), 0, 0)
        self.assertEqual(
            [entry for entry in systemd_command if entry.startswith("--setenv=")],
            ["--setenv=SALIAS_RESOURCE_CAPPED=1"],
        )
        self.assertEqual(systemd_command[-len(command):], command)

    def test_environment_records_and_verifies_setenv(self):
        client = self.output / "client"
        client.write_bytes(b"one")
        client.chmod(0o755)

        class Args:
            groups = 6
            rounds = 20
            messages = 100
            payload = 64
            capacity = 1048576
            interval_seconds = 0.0
            client = None
            setenv = ["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"]

        args = Args()
        args.client = client
        runner.write_environment(args, self.output)
        environment = json.loads((self.output / "environment.json").read_text())
        self.assertEqual(
            environment["environment_variables"], ["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"]
        )
        runner.verify_environment(args, self.output)
        args.setenv = []
        with self.assertRaisesRegex(ValueError, "environment differs"):
            runner.verify_environment(args, self.output)

    def test_analysis_reports_interval_rates_and_runs(self):
        schedules = []
        for interval, _, _, sleep_seconds in runner.SLEEP_SCHEDULE:
            schedules.append((interval, sleep_seconds, [16.0, 16.0]))
            schedules.append((interval, sleep_seconds, [16.0, 6.0]))
            for _ in range(3):
                schedules.append((interval, sleep_seconds, [6.0, 6.0]))
        self.build(schedules)
        runner_data = json.loads((self.output / "summary.json").read_text())
        result = summarize(runner_data)
        self.assertEqual(runner_data, schedules_to_data(schedules))
        self.assertEqual(result["high_mode_count"], 12)
        self.assertAlmostEqual(result["by_interval"]["no_sleep"]["high_mode_rate"], 30.0)
        self.assertAlmostEqual(result["by_interval"]["light_1s"]["high_mode_rate"], 30.0)
        self.assertEqual(result["transitions"]["runs"][0], {"state": "H", "length": 3})
        self.assertEqual(result["transitions"]["max_high_run"], 3)
        self.assertEqual(len(result["round_positions"]), 2)

    def test_round_position_bias_is_reported(self):
        self.build([
            ("no_sleep", 0.0, [16.0, 6.0]),
            ("no_sleep", 0.0, [16.0, 6.0]),
        ])
        result = summarize(json.loads((self.output / "summary.json").read_text()))
        self.assertEqual(result["round_positions"][0]["high_mode_rate"], 100.0)
        self.assertEqual(result["round_positions"][1]["high_mode_rate"], 0.0)

    def test_low_mode_admission_gate_aborts_on_sustained_regression(self):
        class Args:
            consecutive_low = 0
            admission_highs = 0
            max_consecutive_low = 3

        args = Args()
        runner.admit_round(args, 0, 0, 6.0)
        runner.admit_round(args, 0, 1, 4.0)
        with self.assertRaisesRegex(RuntimeError, "Baseline admission failed"):
            runner.admit_round(args, 0, 2, 5.0)

    def test_admission_gate_resets_after_high_mode(self):
        class Args:
            consecutive_low = 0
            admission_highs = 0
            max_consecutive_low = 3

        args = Args()
        runner.admit_round(args, 0, 0, 6.0)
        runner.admit_round(args, 0, 1, 6.0)
        runner.admit_round(args, 0, 2, 16.0)
        self.assertEqual(args.consecutive_low, 0)

    def test_admission_gate_allows_low_run_after_initial_high_mode(self):
        class Args:
            consecutive_low = 0
            admission_highs = 0
            max_consecutive_low = 3

        args = Args()
        runner.admit_round(args, 0, 0, 16.0)
        runner.admit_round(args, 0, 1, 6.0)
        runner.admit_round(args, 0, 2, 6.0)
        runner.admit_round(args, 0, 3, 6.0)
        self.assertEqual(args.admission_highs, 1)
        self.assertEqual(args.consecutive_low, 3)

    def test_incomplete_experiment_cannot_be_reported_as_complete(self):
        self.build([("no_sleep", 0.0, [6.0, 6.0])])
        (self.output / "environment.json").write_text(json.dumps({"groups": 20}))
        result = summarize(load_summary(self.output), {"groups": 20})
        self.assertFalse(result["complete"])
        self.assertEqual(result["expected_groups"], 20)

    def test_environment_threshold_is_used(self):
        self.build([("no_sleep", 0.0, [14.5, 16.0])])
        result = summarize(
            load_summary(self.output),
            {"groups": 1, "high_mode_threshold_mmsg_s": 14.0},
        )
        self.assertEqual(result["threshold_mmsg_s"], 14.0)
        self.assertEqual(result["high_mode_count"], 2)
        self.assertEqual(result["by_interval"]["no_sleep"]["high_count"], 2)
        self.assertEqual(result["round_positions"][0]["high_count"], 1)


def schedules_to_data(schedules):
    data = []
    for group_idx, (interval, sleep_seconds, rates) in enumerate(schedules):
        data.append({
            "group": group_idx,
            "interval": interval,
            "sleep_duration": sleep_seconds,
            "throughputs": rates,
            "high_mode_count": sum(rate >= runner.HIGH_MODE_THRESHOLD_MMSG_S
                                   for rate in rates),
            "median": sorted(rates)[len(rates) // 2],
            "rounds": len(rates),
        })
    return data


if __name__ == "__main__":
    unittest.main()
